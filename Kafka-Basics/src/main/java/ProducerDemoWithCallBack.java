import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
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
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Topic : "+recordMetadata.topic() +
                            "\nOffset : "+recordMetadata.offset());
                } else {
                    logger.error("exception", e);
                }
            }
        });

        //flush and close produce, else consumer will not get message
        producer.flush();
        producer.close();
    };
}
