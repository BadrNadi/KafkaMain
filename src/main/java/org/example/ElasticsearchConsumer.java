package org.example;

import com.google.gson.JsonParser;
import nl.altindag.ssl.SSLFactory;
import org.apache.kafka.clients.consumer.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
public class ElasticsearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        SSLFactory sslFactory = SSLFactory.builder()
                .withUnsafeTrustMaterial()
                .withUnsafeHostnameVerifier()
                .build();
        RestClient restClient = RestClient
                .builder(new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(httpClientBuilder ->httpClientBuilder.setSSLContext(sslFactory.getSslContext())).build();
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();
        System.out.println(userInfo);
        if (userInfo == null) {
            System.out.println(connUri.getHost());
            System.out.println(connUri.getHost());
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
    public static KafkaConsumer<String,String> kafkaconsumer(){
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"secondGroup");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // without a revoke when a consumer get down
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        return new  KafkaConsumer<>(properties);
    }
    private static String extractId(String value) {
        return JsonParser.parseString(value).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }
    public static void main(String[] args) throws IOException {
        RestHighLevelClient elasticsearchConsumer = createOpenSearchClient();
        CreateIndexRequest createIndexRequest=new CreateIndexRequest("kafkawikimediaconsumer2");
        elasticsearchConsumer.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        KafkaConsumer<String,String> kafkaConsumer= kafkaconsumer();

        kafkaConsumer.subscribe(Collections.singleton("wikimedia"));
        while(true){
            //block the thread for duration 100
            ConsumerRecords<String, String> consumerRecord=kafkaConsumer.poll(Duration.ofMillis(1000));
            System.out.println("Received records : "+consumerRecord.count());
            for(  ConsumerRecord<String, String> consumer:consumerRecord){
                String id =extractId(consumer.value());
                System.out.println(id);
                try {
                    IndexRequest indexRequest=new IndexRequest("").source(consumer.value(), XContentType.JSON).id(id);
                    IndexResponse indexResponse=elasticsearchConsumer.index(indexRequest,RequestOptions.DEFAULT);
                    System.out.println(indexResponse.getId());

                }catch (Exception e){
                    e.printStackTrace();
                }
            }


        }

    }


}
