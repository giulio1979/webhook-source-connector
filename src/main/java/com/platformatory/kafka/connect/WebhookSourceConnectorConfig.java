package com.platformatory.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;



public class WebhookSourceConnectorConfig extends AbstractConfig {

  public static final String DEFAULT_TOPIC_CONFIG = "topic.default";
  public static final String DEFAULT_TOPIC_DOC = "Default topic to write to for DLQ";

  public static final String TOPIC_HEADER_CONFIG = "topic.header";
  public static final String TOPIC_HEADER_DOC = "Header for determining the topic";

  public static final String KEY_HEADER_CONFIG = "key.header";
  public static final String KEY_HEADER_DOC = "Header for determining the key";

  public static final String PORT_CONFIG = "port";
  public static final String PORT_DOC = "Port for HTTP server";

  public static final String VALIDATOR_CLASS_CONFIG = "validator.class";
  public static final String VALIDATOR_CLASS_DOC = "Validator Class for webhook request validation";

  public static final String POLL_INTERVAL_CONFIG = "poll.interval";
  public static final Long POLL_INTERVAL_DEFAULT = 5000L;
  public static final String POLL_INTERVAL_DOC = "Poll interval for the connect to produce to Kafka";

  public WebhookSourceConnectorConfig(Map<String, String> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    return new ConfigDef()
            .define(DEFAULT_TOPIC_CONFIG, Type.STRING, Importance.HIGH, DEFAULT_TOPIC_DOC)
            .define(TOPIC_HEADER_CONFIG, Type.STRING, Importance.HIGH, TOPIC_HEADER_DOC)
            .define(KEY_HEADER_CONFIG, Type.STRING, Importance.HIGH, KEY_HEADER_DOC)
            .define(PORT_CONFIG, Type.INT, Importance.HIGH, PORT_DOC)
            .define(VALIDATOR_CLASS_CONFIG, Type.STRING, Importance.LOW, VALIDATOR_CLASS_DOC)
            .define(POLL_INTERVAL_CONFIG, Type.LONG, POLL_INTERVAL_DEFAULT, Importance.HIGH, POLL_INTERVAL_DOC);
  }

  public String getDefaultTopic() {
    return this.getString(DEFAULT_TOPIC_CONFIG);
  }

  public String getTopicHeader() {
    return this.getString(TOPIC_HEADER_CONFIG);
  }

  public String getKeyHeader() {
    return this.getString(KEY_HEADER_CONFIG);
  }

  public int getPort() {
    return this.getInt(PORT_CONFIG);
  }

  public long getPollInterval() {
    return this.getLong(POLL_INTERVAL_CONFIG);
  }

  public String getValidatorClass() {
    return this.getString(VALIDATOR_CLASS_CONFIG);
  }


}
