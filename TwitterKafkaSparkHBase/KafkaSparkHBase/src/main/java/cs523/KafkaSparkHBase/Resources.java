package cs523.KafkaSparkHBase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Resources {

	public String TOPIC;
	public String PORT;
	public String SERVER;
	public String GROUP;
	public String AUTO_COMMIT;
	public String AUTO_COMMIT_INTERVAL;
	public String SESSION_TIMEOUT;
	public String DESERIALIZER;
	
	public Resources() throws IOException {
		InputStream inStream = null;
		try{
			Properties props = new Properties();
			inStream = getClass().getClassLoader().getResourceAsStream(PropertiesConstant.CONFIG_FILE);
			if (inStream != null){
				props.load(inStream);
				this.TOPIC = props.getProperty("TOPIC");
				this.PORT = props.getProperty("PORT");
				this.SERVER = props.getProperty("SERVER");
				this.GROUP = props.getProperty("GROUP");
				this.AUTO_COMMIT = props.getProperty("AUTO_COMMIT");
				this.AUTO_COMMIT_INTERVAL = props.getProperty("AUTO_COMMIT_INTERVAL");
				this.SESSION_TIMEOUT = props.getProperty("SESSION_TIMEOUT");
				this.DESERIALIZER = props.getProperty("DESERIALIZER");
			}
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
		finally {
			if (inStream != null) inStream.close();
		}
	}
}