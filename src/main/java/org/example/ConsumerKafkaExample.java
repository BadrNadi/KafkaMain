package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaExample {
   private static final Logger log= LoggerFactory.getLogger(ConsumerKafkaExample.class.getSimpleName());
    public static void main(String[] args) {

        System.out.println("Hello world!");
        log.info("Kafka Producer has been started");
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"secondGroup");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // without a revoke when a consumer get down
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList( "kafkajava3"));
        while (true){
            //block the thread for duration 100
            ConsumerRecords<String, String> consumerRecord=kafkaConsumer.poll(Duration.ofMillis(100));
             for(  ConsumerRecord<String, String> consumer:consumerRecord){
                 System.out.println("Received Meta data : " + "\n" +
                         "Topic :" + consumer.topic() + "\n" +
                         "Partition :" + consumer.partition() + "\n" +
                         "Offset :" + consumer.offset() + "\n"
                 );
             }

        }



    }
}