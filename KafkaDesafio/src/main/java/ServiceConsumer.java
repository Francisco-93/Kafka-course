import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.awt.event.KeyListener;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ServiceConsumer {

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Collections.singletonList("MENSAGEM_INSTANTANEA"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                for(var msg : records){
                    System.out.println(msg.value());
                }
            }
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "COMPRA_APROVADA_GROUP");

        return properties;
    }
}
