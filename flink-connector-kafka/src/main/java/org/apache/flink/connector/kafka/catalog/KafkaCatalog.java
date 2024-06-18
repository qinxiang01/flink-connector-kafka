package org.apache.flink.connector.kafka.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.GlobalCache;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static org.apache.flink.connector.kafka.catalog.factory.KafkaCatalogFactoryOptions.IDENTIFIER;
import static org.apache.flink.connector.kafka.catalog.factory.KafkaCatalogFactoryOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KafkaCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalog.class);
    private final String KAFKA_GROUP_ID = "flink-catalog-%s-%s";


    private transient AdminClient adminClient;
    private transient Properties baseProperties;

    private final String bootstrapServers;
    private final String valueFormat;
    private final String keyFieldsPrefix;


    public KafkaCatalog(
            ClassLoader userClassLoader,
            String name,
            String bootstrapServers,
            String defaultDatabase,
            String valueFormat,
            ReadableConfig options
    ) {
        super(name, defaultDatabase);
        checkNotNull(userClassLoader);
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(bootstrapServers));
        checkArgument(KafkaValuesFormat.checkFormat(valueFormat), "Unsupported value format:" + valueFormat);
        this.bootstrapServers = bootstrapServers;
        this.valueFormat = valueFormat;
        this.keyFieldsPrefix = options.get(KEY_FIELDS_PREFIX) == null ? KEY_FIELDS_PREFIX.defaultValue() : options.get(KEY_FIELDS_PREFIX);
        baseProperties = new Properties();
        baseProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        baseProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getName());
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Opening Kafka Catalog");
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Closing Kafka Catalog");
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        String defaultDatabase = getDefaultDatabase();
        List<String> databases = new ArrayList<>();
        databases.add(defaultDatabase);
        return databases;
    }

    @Override
    public CatalogDatabase getDatabase(String s) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return Objects.equals(getDefaultDatabase(), databaseName);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(!StringUtils.isNullOrWhitespaceOnly(databaseName), "Database name can't be null or empty");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        AdminClient client = getAdminClient();
        try {
            Set<String> topicNames = client.listTopics().names().get();
            ArrayList<String> topicList = new ArrayList<>(topicNames);
            Collections.sort(topicList);
            return topicList;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list topic names", e);
        }
    }


    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        String tableName = objectPath.getObjectName();
        try {
            return listTables(objectPath.getDatabaseName()).contains(tableName);
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath topicPath) throws TableNotExistException, CatalogException {
        LOG.info(" create kafka catalog table :{}", topicPath.getFullName());
        // 获取主键 ，使用partition和offset作为key
        if (!tableExists(topicPath)) {
            throw new TableNotExistException(getName(), topicPath);
        }

        String topicName = topicPath.getObjectName();

        List<ConsumerRecord<String, String>> records = consumerMessage(topicName);
        String[] columnNames = null;
        DataType[] types = null;
        if (records == null || records.isEmpty() || records.get(0) == null || records.get(0).value() == null) {
            // 从Kafka没有获取到数据，就从前置缓存的SQL语句中的字段获取数据,内置逻辑：详见：SqlNodeToOperationConversion.convert
            Object object = GlobalCache.get();
            if (object instanceof Map) {
                Map tableMap = (Map) object;
                List colList = (List) tableMap.get(getName() + "=" + topicPath.getDatabaseName() + "=" + topicPath.getObjectName());
                if (null == colList) {
                    throw new RuntimeException("Failed to find record in table " + topicPath.getDatabaseName() + "." + topicPath.getObjectName());
                }
                columnNames = new String[colList.size()];
                types = new DataType[colList.size()];
                for (int i = 0; i < colList.size(); i++) {
                    String col = (String) colList.get(i);
                    columnNames[i] = col;
                    // 默认全部是字符串
                    types[i] = DataTypes.STRING();
                }
            } else {
                throw new RuntimeException("Failed to find record in table " + topicPath.getDatabaseName() + "." + topicPath.getObjectName());
            }
        } else {
            // 从Kafka获取到的数据来解析字段和类型
            ConsumerRecord<String, String> record = records.get(0);
            String value = record.value();
            ObjectMapper objectMapper = new ObjectMapper();
            Map recoderMap = null;
            try {
                recoderMap = objectMapper.readValue(value, Map.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            columnNames = new String[recoderMap.size()];
            types = new DataType[recoderMap.size()];
            int i = 0;
            for (Object key : recoderMap.keySet()) {
                Object object = recoderMap.get(key);
                columnNames[i] = key.toString();
                types[i] = KafkaTypeMapper.mapping(object);
                i++;
            }
        }
        Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);

        Schema tableSchema = schemaBuilder.build();

        // kafka连接器的参数
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR.key(), IDENTIFIER);
        props.put(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS.key(), bootstrapServers);
        props.put(KafkaConnectorOptions.VALUE_FORMAT.key(), valueFormat.toLowerCase());
        props.put(KafkaConnectorOptions.TOPIC.key(), topicName);
        props.put(KafkaConnectorOptions.PROPS_GROUP_ID.key(), String.format(KAFKA_GROUP_ID, getName(), "connector"));
        // upsert-kafka 不支持的参数
        props.put(KafkaConnectorOptions.SCAN_STARTUP_MODE.key(), KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET.toString());

        // 手动处理，不交给kafka
        props.put(KafkaConnectorOptions.KEY_FIELDS_PREFIX.key(), keyFieldsPrefix);
        props.put(KafkaConnectorOptions.VALUE_FIELDS_INCLUDE.key(), KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY.toString());
        return CatalogTable.of(tableSchema, null, Lists.newArrayList(), props);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath objectPath, List<Expression> list) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    //--------------------    Unsupported --------------------
    @Override
    public void createDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException("Unsupported dropDatabase()");
    }

    @Override
    public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported alterDatabase()");
    }

    @Override
    public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported listViews()");
    }

    @Override
    public void dropTable(ObjectPath objectPath, boolean b) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported  dropTable()");
    }

    @Override
    public void renameTable(ObjectPath objectPath, String s, boolean b) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported  renameTable()");
    }

    @Override
    public void createTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported  createTable()");
    }

    @Override
    public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported  alterTable()");
    }


    @Override
    public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

    }

    @Override
    public void dropPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean b) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public List<String> listFunctions(String s) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath objectPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public void dropFunction(ObjectPath objectPath, boolean b) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean b) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws PartitionNotExistException, CatalogException {


    }


    private AdminClient getAdminClient() {
        if (adminClient == null) {
            Properties adminClientProps = new Properties();
            KafkaPropertiesUtil.copyProperties(baseProperties, adminClientProps);
            String clientIdPrefix =
                    adminClientProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
            adminClientProps.setProperty(
                    CommonClientConfigs.CLIENT_ID_CONFIG,
                    clientIdPrefix + "-single-cluster-topic-metadata-service");
            adminClient = AdminClient.create(adminClientProps);
        }
        return adminClient;
    }


    private List<ConsumerRecord<String, String>> consumerMessage(String topic) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.format(KAFKA_GROUP_ID, getName(), "ddl" + RandomUtils.nextInt(1, 1000000000)));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            int count = 0;
            while (true) {
                count++;
                if (count >= 20) {
                    break;
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    return Collections.singletonList(record);
                }
            }
        }
        return null;
    }
}
