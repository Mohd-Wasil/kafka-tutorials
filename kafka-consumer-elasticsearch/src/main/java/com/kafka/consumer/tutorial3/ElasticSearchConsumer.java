package com.kafka.consumer.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static RestHighLevelClient createClient() {

        // replace with your own credentials
        String hostname = "kafka-poc-6908695280.us-east-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "xs9do5xktt"; // needed only for bonsai
        String password = "u464we62vr"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = createClient();
        String jsonString = "{ \"foo\": \"bar\" }";


        final String topic = "twitter_tweets";
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(topic);

        //poll for new data
        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100)); //poll is deprecated _ duration is good
            Integer recordCount = records.count();
            logger.info("Recieved from kafka : batches records : " + records.count());

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {

                //2 generics IDs to make consumer idempotent -
                // 1. kafka generic ID
                //String id = record.topic() + "_" + record.partition() +"_" +  record.offset(); // unique value combination
                // 2. from twitter feed specifc ID
                String id = extractIdFromTweet(record.value());
                //insert data consumed from kafka topic into elastic search - bonzai
                //record.value() - tweets
                IndexRequest indexRequest = new IndexRequest
                        ("twitter",
                                "tweets",
                                id//  to make consumer message processing idempotent
                        ).source(record.value(), XContentType.JSON);
                bulkRequest.add(indexRequest);
                //single request execution
                // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                //String id = indexResponse.getId();
                logger.info("id : " + id);

            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("committing the offsets ...");
                kafkaConsumer.commitSync();
                logger.info("offsets have been committed");
                Thread.sleep(100);
            }
        }

        //close the client ggracefully
        //  client.close();

    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        //gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        final String bootStrapServers = "localhost:9092";
        final String groupId = "kafka-demo-elastic-search";

        //set properties, consumer config
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"latest"/none throw errors
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // to disable auto comit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200"); // max records fetched in single poll request

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //suscribe to topic
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
