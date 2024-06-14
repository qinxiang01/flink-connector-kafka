package org.apache.flink.connector.kafka.catalog.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.catalog.KafkaCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.*;

public class KafkaCatalogFactory implements CatalogFactory {
    @Override
    public String factoryIdentifier() {
        return KafkaCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();


        Properties baseProperties = new Properties();
        ReadableConfig options = helper.getOptions();
        return new KafkaCatalog(
                context.getClassLoader()
                ,context.getName()
                ,helper.getOptions().get(KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS)
                ,helper.getOptions().get(KafkaCatalogFactoryOptions.DEFAULT_DATABASE)
                ,helper.getOptions().get(KafkaCatalogFactoryOptions.VALUES_FORMAT)
                ,options
        );

    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS);
        options.add(KafkaCatalogFactoryOptions.DEFAULT_DATABASE);
        options.add(KafkaCatalogFactoryOptions.VALUES_FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaCatalogFactoryOptions.KEY_FIELDS_PREFIX);
        options.add(KafkaCatalogFactoryOptions.INFER_SCHEMA_PRIMITIVE_AS_STRING);
        options.add(KafkaCatalogFactoryOptions.MAX_FETCH_RECORDS);

        options.add(KafkaCatalogFactoryOptions.MAX_FETCH_RECORDS);
        options.add(KafkaCatalogFactoryOptions.MAX_FETCH_RECORDS);
        options.add(KafkaCatalogFactoryOptions.MAX_FETCH_RECORDS);
        return options;
    }

}
