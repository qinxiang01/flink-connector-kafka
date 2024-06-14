package org.apache.flink.connector.kafka.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalog.class);
    public static DataType mapping(Object object){
        if (null == object){
            return DataTypes.STRING();
        }
        if (object instanceof String){
            return DataTypes.STRING();
        }
        else if (object instanceof Integer){
            return DataTypes.INT();
        }
        else if (object instanceof Long){
            return DataTypes.BIGINT();
        }
        else if (object instanceof Double){
            return DataTypes.DOUBLE();
        }
        else if (object instanceof Float){
            return DataTypes.FLOAT();
        }
        else if (object instanceof Boolean){
            return DataTypes.BOOLEAN();
        }
        else if (object instanceof Short){
            return DataTypes.SMALLINT();
        }
        else if (object instanceof Byte){
            return DataTypes.TINYINT();
        }
        else if (object instanceof Character){
            return DataTypes.STRING();
        } else if (object instanceof Map){
            return DataTypes.STRING();
        }else {
            LOG.info("Unknown object type: " + object.getClass().getName());
            return DataTypes.STRING();
        }
    }
}
