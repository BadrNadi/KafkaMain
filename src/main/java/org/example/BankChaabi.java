package org.example;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankChaabi {
   private static final Logger log= LoggerFactory.getLogger(BankChaabi.class.getSimpleName());
    public static void main(String[] args) {

        System.out.println("Hello world!");
        log.info("Kafka Producer has been started");
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String,String> Producer=new KafkaProducer<String, String>(properties);
        int i=0;
        while(true){
            System.out.println("processed id is "+ i);
            try {
                Producer.send(newTransaction("john"));
                Thread.sleep(100);
                Producer.send(newTransaction("badr"));
                Thread.sleep(100);
                Producer.send(newTransaction("aissa"));
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }



    }

    private static ProducerRecord<String, String> newTransaction(String badr) {
        ObjectNode objectNode= JsonNodeFactory.instance.objectNode();
        Integer amount= ThreadLocalRandom.current().nextInt(0,100);
        Instant instant=Instant.now();
        objectNode.put("name",badr);
        objectNode.put("amount",amount);
        objectNode.put("time",instant.toString());
        return new ProducerRecord<>("bank",badr,objectNode.toString());
    }
}