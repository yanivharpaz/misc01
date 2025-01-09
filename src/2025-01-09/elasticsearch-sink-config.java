package org.example.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {

    public static final String ELASTICSEARCH_HOST = "elasticsearch.host";
    public static final String ELASTICSEARCH_PORT = "elasticsearch.port";
    public static final String ELASTICSEARCH_SCHEME = "elasticsearch.scheme";
    public static final String INDEX_PREFIX = "index.prefix";
    public static final String BATCH_SIZE = "batch.size";
    public static final String BATCH_TIMEOUT_MS = "batch.timeout.ms";
    public static final String DLQ_TOPIC = "dlq.topic";

    public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            ELASTICSEARCH_HOST,
            Type.STRING,
            "localhost",
            Importance.HIGH,
            "Elasticsearch host"
        )
        .define(
            ELASTICSEARCH_PORT,
            Type.INT,
            9200,
            Importance.HIGH,
            "Elasticsearch port"
        )
        .define(
            ELASTICSEARCH_SCHEME,
            Type.STRING,
            "http",
            Importance.MEDIUM,
            "Elasticsearch connection scheme (http/https)"
        )
        .define(
            INDEX_PREFIX,
            Type.STRING,
            "prd_a_",
            Importance.HIGH,
            "Prefix for Elasticsearch indices"
        )
        .define(
            BATCH_SIZE,
            Type.INT,
            1000,
            Importance.MEDIUM,
            "Number of records to batch before indexing"
        )
        .define(
            BATCH_TIMEOUT_MS,
            Type.LONG,
            5000,
            Importance.MEDIUM,
            "Maximum time to wait before indexing a partial batch"
        )
        .define(
            DLQ_TOPIC,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "Topic for dead letter queue (optional)"
        );
}