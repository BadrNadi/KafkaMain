package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKafkaExample {
   private static final Logger log= LoggerFactory.getLogger(ProducerKafkaExample.class.getSimpleName());
    public static void main(String[] args) {

        System.out.println("Hello world!");
        log.info("Kafka Producer has been started");
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> Producer=new KafkaProducer<String, String>(properties);

        ProducerRecord<String,String> record=new ProducerRecord<>("kafkajava2","Hello Badr");
        Producer.send(record);
        Producer.flush();
        Producer.close();


    }
}