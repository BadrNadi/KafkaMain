package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.internals.KTableAggregate;

import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;


public class WorldCount {
    public static void main(String[] args) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        StreamsBuilder streamsBuilder=new StreamsBuilder();
//        KStream<String, String> source = streamsBuilder.stream("input");
////        KTable<String,Long> kTable = source.mapValues(text ->text.toLowerCase()).
////                flatMapValues(lowertext -> Arrays.asList(lowertext.split(" "))).
////                selectKey((key,value) ->value).groupByKey().count();
//        KStream<String, String> source2 = source.flatMapValues(ligne-> Arrays.asList(ligne.split(","))).
//                selectKey((k,v)->v).filter((k,v)->v.equals("red") || v.equals("green"));
//        source2.to("red");
//        KTable<String,String> kTable=streamsBuilder.table("red");
//        KTable<String,Long> kTable1=kTable.groupBy((k,v)->new KeyValue<>(v,v)).count();
//        kTable1.toStream().map((k,v)->new KeyValue<>(k.toString(),v)).to("favorite", Produced.with(Serdes.String(), Serdes.Long()));
//
//
////        kTable.toStream().map((k,v)->new KeyValue<>(k.toString(),v)).to("outputstream", Produced.with(Serdes.String(), Serdes.Long()));
//        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),properties);
//        kafkaStreams.start();
//        System.out.println(kafkaStreams.toString());
//        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lineStream = builder.stream("input");
        KStream<String, String> filteredStream = lineStream.filter((key, val) -> val.contains(","))
                .selectKey((key, val) -> val.split(",")[0])
                .mapValues((val) -> val.split(",")[1].toLowerCase())
                .filter((key, val) -> val.matches("green|blue|red")).peek((k,v)-> System.out.println("key is "+k+" value is " +v));;

        filteredStream.to("red");

        KTable<String, String> favColorTable = builder.table("red");


        /**
         * With KTables the last change is maintained so that we can retrieve the last value and look up the count for the last value
         */
        KTable<String, Long> colorCountedTable = favColorTable
                .groupBy((key, val) -> new KeyValue<>(val, val))
                .count();
        colorCountedTable.toStream().map((k,v)->new KeyValue<>(k.toString(),v)).to("outputstream3", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams=new KafkaStreams(builder.build(),properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();


        System.out.println("Topology: " + kafkaStreams.toString());

        /**
         * Finally clean up the stream on shutdown graceully with a shutdownhook
         */
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
