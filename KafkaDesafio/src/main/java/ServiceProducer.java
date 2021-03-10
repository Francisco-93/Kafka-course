import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ServiceProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Scanner sc = new Scanner(System.in);

        var producer = new KafkaProducer<String, String>(getProperties());
        String value = "";
        int cont = 0;
        while(!value.equals("sair")) {
           String key = "Mensagem " + ++cont +" "+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
           value = key + "\n" +sc.nextLine();
           var record = new ProducerRecord<>("MENSAGEM_INSTANTANEA", key, value);
           producer.send(record, new Callback() {
               @Override
               public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                   if (e != null) {
                       System.out.println(e.getMessage());
                       e.printStackTrace();
                       return;
                   }
                   System.out.println("Mensagem Enviada com Sucesso " + recordMetadata.topic() + " na partition " + recordMetadata.partition()
                           + " no offset " + recordMetadata.offset() + " no timestamp " + recordMetadata.timestamp());
               }
           }).get();
       }
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        return properties;
    }

}
