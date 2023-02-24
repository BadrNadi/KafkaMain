package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBacksKafkaExample {
   private static final Logger log= LoggerFactory.getLogger(ProducerWithCallBacksKafkaExample.class.getSimpleName());
    public static void main(String[] args) {

        System.out.println("Hello Kafka!");
        log.info("Kafka Producer has been started");
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //batch size and also lenger time
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");


        KafkaProducer<String,String> Producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("kafkajava3","id_"+i , "Hello_"+i);
            Producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            System.out.println("Received Meta data : " + "\n" +
                                    "Topic :" + recordMetadata.topic() + "\n" +
                                    "Partition :" + recordMetadata.partition() + "\n" +
                                    "Offset :" + recordMetadata.offset() + "\n"
                            );
                        }
                    }
            );
        }
        Producer.flush();
        Producer.close();


    }
}