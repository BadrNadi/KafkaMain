package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventSource.Builder;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerKafkaWikimediaExample {
   private static final Logger log= LoggerFactory.getLogger(ProducerKafkaWikimediaExample.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {

        System.out.println("Hello world!");
        log.info("Kafka Producer has been started");
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> Producer=new KafkaProducer<String, String>(properties);

       String topic ="wikimedia";
        EventHandler eventHandler=new WikimediaChangeHandler(Producer,topic);
        String url="https://stream.wikimedia.org/v2/stream/recentchange";
        System.out.println("1---------------------------------------------");
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        System.out.println(builder.build().getState());
        EventSource eventSource = builder.build();


        // start the producer in another thread
        eventSource.start();
        System.out.println(eventSource.getUri());

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);


    }
}