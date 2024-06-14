/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.catalog.factory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** {@link ConfigOption}s for {@link org.apache.flink.connector.kafka.catalog.KafkaCatalog}. */
@Internal
public class KafkaCatalogFactoryOptions {

    public static final String IDENTIFIER = "kafka";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("properties.bootstrap.servers" ).stringType().noDefaultValue();

      public static final ConfigOption<String> VALUES_FORMAT =
            ConfigOptions.key("value.format" ).stringType().defaultValue("JSON");

    /**
     *     自定义添加到消息键（Key）解析出字段名称的前缀，来避免Kafka消息键解析后的命名冲突问题。
     *     默认值为key_。例如，如果您的key字段名为a，则系统默认解析key后的字段名称为key_a。
     *     说明
     *     key.fields-prefix的配置值不可以是value.fields-prefix的配置值的前缀。例如value.fields-prefix配置为test1_value_，则key.fields-prefix不可以配置为test1_。
     */
    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix").stringType().defaultValue("key_");

    /**
     * 自定义添加到消息体（Value）解析出字段名称的前缀，来避免Kafka消息体解析后的命名冲突问题。
     * 默认值为value_。例如，如果您的value字段名为b，则系统默认解析value后的字段名称为value_b。
     * 说明
     * value.fields-prefix的配置值不可以是key.fields-prefix的配置值的前缀。例如key.fields-prefix配置为test2_value_，则value.fields-prefix不可以配置为test2_。
     */
    public static final ConfigOption<String> VALUE_FIELDS_PREFIX =
            ConfigOptions.key("value.fields-prefix" ).stringType().defaultValue("value_");

    /**
     * 解析JSON格式消息体（Value）时，是否递归式地展开JSON中的嵌套列
     * 参数取值如下：
     *    true：递归式展开。
     *    对于被展开的列，Flink使用索引该值的路径作为名字。例如，对于{"nested": {"col": true}} 中的列col，它展开后的名字为nested.col。
     *    说明
     *    设置为true时，建议和CREATE TABLE AS（CTAS）语句配合使用，目前暂不支持其它DML语句自动展开嵌套列。
     *    false（默认值）：将嵌套类型当作String处理。
     */
    public static final ConfigOption<Boolean> infer_schema_flatten_nested_columns_enable =
            ConfigOptions.key("infer-schema.flatten-nested-columns.enable" ).booleanType().defaultValue(false);

    /**
     * 解析JSON格式消息体（Value）时，是否推导所有基本类型为String类型。
     * 参数取值如下：
     * true：推导所有基本类型为String。
     * false（默认值）：按照基本规则进行推导。
     */
    public static final ConfigOption<Boolean> INFER_SCHEMA_PRIMITIVE_AS_STRING =
            ConfigOptions.key("infer-schema.primitive-as-string" ).booleanType().defaultValue(false);

    /**
     * 解析JSON格式消息时，最多尝试消费的消息数量。
     */
    public static final ConfigOption<Integer> MAX_FETCH_RECORDS =
            ConfigOptions.key("max.fetch.records").intType().defaultValue(1);

    public static final ConfigOption<String> PROPERTIES_SECURITY_PROTOCOL =
            ConfigOptions.key("properties.security.protocol").stringType().noDefaultValue();

    public static final ConfigOption<String> PROPERTIES_SASL_MECHANISM =
            ConfigOptions.key("properties.sasl.mechanism").stringType().noDefaultValue();

    public static final ConfigOption<String> PROPERTIES_SASL_JAAS_CONFIG =
            ConfigOptions.key("properties.sasl.jaas.config").stringType().noDefaultValue();

    public static final ConfigOption<String> TOPIC_NAME =
            ConfigOptions.key("topic-name").stringType().noDefaultValue();


    private KafkaCatalogFactoryOptions() {}
}
