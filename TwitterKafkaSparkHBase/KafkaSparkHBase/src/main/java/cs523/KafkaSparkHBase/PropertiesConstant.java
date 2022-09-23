package cs523.KafkaSparkHBase;

public abstract class PropertiesConstant {
	
	protected static final String CONFIG_FILE = "config.properties";
	protected static final String GROUP_ID = "group.id";
	protected static final String BOOTSTRAP_SERVERS ="bootstrap.servers";
	protected static final String AUTO_COMMIT = "enable.auto.commit";
	protected static final String AUTO_COMMIT_INTERVAL = "auto.commit.interval.ms";
	protected static final String SESSION_TIMEOUT = "session.timeout.ms";
	protected static final String KEY_DESERIALIZER = "key.deserializer";
	protected static final String VALUE_DESERIALIZER = "value.deserializer";

}