import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        final String bootstrapServer = "127.0.0.1:9092";

        //produce properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        //kafka produce
        KafkaProducer producer = new KafkaProducer(properties);

        //producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first-topic",
                "hello from java-102");
        //send message
        producer.send(record);

        //flush and close produce, else consumer will not get message
        producer.flush();
        producer.close();
    };
}
