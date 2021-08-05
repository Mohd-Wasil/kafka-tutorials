package com.example.simplestuff.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

    public static void main(String[] args) {
        final String bootStrapServers = "localhost:9092";

        final String topic = "states";
        //set properties, consumer config
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //"latest"/none throw errors

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //suscribe consumer to topic (s) - NOT needed
        // consumer.subscribe(Collections.singleton(topic)); //collection of topics
        //??

        //assign and seek  used to replay and fetch specifc message
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign((Arrays.asList(topicPartitionToReadFrom)));
        long offSetToReadFrom = 15L;
        //seek
        consumer.seek(topicPartitionToReadFrom, offSetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;
        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //poll is deprecated _ duration is good
            for (ConsumerRecord<String, String> record : records) {
                logger.info("consumer records recieved : " + "\n"
                        + "key : " + record.key() + "\n"
                        + "value : " + record.value() + "\n"
                        + "partition : " + record.partition() + "\n"
                        + "offsets : " + record.offset() + "\n");
                ++numberOfMessageReadSoFar;
                if (numberOfMessageReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }

            }
        }
        logger.info("exisitn the app..");
    }
}
