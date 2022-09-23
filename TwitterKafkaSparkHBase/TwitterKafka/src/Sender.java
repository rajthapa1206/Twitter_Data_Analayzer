import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class Sender {

	 public static Producer<String, String> initializeProducer() {
		 Properties prop = new Properties();
	     prop.put(Constant.BOOTSTRAP_SERVERS_PROP, Constant.BOOTSTRAP_SERVERS);
	     prop.put(Constant.KEY_SERIALIZER, StringSerializer.class.getName());
	     prop.put(Constant.VALUE_SERIALIZER, StringSerializer.class.getName());
	     return new KafkaProducer<>(prop);
	 }
	 
	 public static void sendTweet(String id, String text) throws Exception {
		 final Producer<String, String> producer = initializeProducer();
	     final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(Constant.TOPIC, id, text);
	     producer.send(producerRecord).get();
	 }
	 
}