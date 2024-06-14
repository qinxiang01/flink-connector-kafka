package org.apache.flink.connector.kafka.catalog;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public enum KafkaValuesFormat {
    JSON;

    public static boolean checkFormat(String format){
        if (StringUtils.isBlank(format)){
            return false;
        }
        return Arrays.stream(KafkaValuesFormat.values()).anyMatch(info -> info.name().equalsIgnoreCase(format));
    }

}
