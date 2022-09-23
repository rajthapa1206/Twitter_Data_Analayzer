package cs523.KafkaSparkHBase;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerKafka {
	
	private KafkaConsumer<String, String> kafkaConsumer;
	private Resources resources;
	private String lastKey = "";
	
	public ConsumerKafka(Resources myResources){
		this.resources = myResources;
		Properties props = new Properties();
	    props.put(PropertiesConstant.BOOTSTRAP_SERVERS, resources.SERVER + ":" + resources.PORT);
	    props.put(PropertiesConstant.GROUP_ID, resources.GROUP);
	    props.put(PropertiesConstant.AUTO_COMMIT, resources.AUTO_COMMIT);
	    props.put(PropertiesConstant.AUTO_COMMIT_INTERVAL, resources.AUTO_COMMIT_INTERVAL);
	    props.put(PropertiesConstant.SESSION_TIMEOUT, resources.SESSION_TIMEOUT);
	    props.put(PropertiesConstant.KEY_DESERIALIZER, resources.DESERIALIZER);
	    props.put(PropertiesConstant.VALUE_DESERIALIZER, resources.DESERIALIZER);
	    this.kafkaConsumer = new KafkaConsumer<String, String>(props);
	    this.kafkaConsumer.subscribe(Arrays.asList(resources.TOPIC));
	}
	
	public void Wait(Predict predictable, ConsumerCallback callback)
	{
		while (predictable.check()) 
	    {
	        @SuppressWarnings("deprecation")
	        ConsumerRecords<String, String> records = this.kafkaConsumer.poll(100);
	        for (ConsumerRecord<String, String> record : records)
	        {
	        	callback.consume(record.key(), record.value(), this.lastKey.compareTo(record.key().toLowerCase()) != 0);
	        	this.lastKey = record.key().toLowerCase();
	        }
	    }
	}
	
	@FunctionalInterface
	public interface Predict 
	{
	    boolean check();
	}
	
	@FunctionalInterface
	public interface ConsumerCallback
	{
	    void consume(String key, String val, boolean new_key );
	}
}