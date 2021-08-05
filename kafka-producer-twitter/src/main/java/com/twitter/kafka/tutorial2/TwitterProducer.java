package com.twitter.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */

    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    //    BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
    String consumerKey = "O7uVUizeWsdKJLOQ20He4LQzy";
    String consumerSecret = "VWd7qqoCCLqQO7WcMP3KHMawjiwPkk4YhVJqYuSuCmKyIXGE9w";
    String token = "1399380763838091264-XpaLuZ0sukc1cwo3jhttdCK4jTIwA3";
    String secret = "bwyksvswO5IVWIv8QxsUbQ1HrAh7CMTpRhyVABE9vTNPp";
    List<String> terms = Lists.newArrayList("modi", "sex");
    public TwitterProducer() {

    }

    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();


    }

    public void run() throws InterruptedException {
        //create a ttwitter client
        Client client = createTwitterClient();
        // Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("stopping apps...");
            logger.info("shutting down client twitter: ");
            client.stop();
            logger.info("shutting down producer");
            producer.close();
            logger.info("done!!");
        }));
        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("something bad happened at kafka producer : ", e);
                        } else {
                            logger.info("message pushed to kafka : " + "\n" +
                                    " Topic : " + recordMetadata.topic() + " \n"
                                    + "Partition : " + recordMetadata.partition() + " \n"
                                    + "OffSet : " + recordMetadata.offset() + " \n"
                                    + "TimeStamp : " + recordMetadata.timestamp() + " \n");
                        }
                    }
                });
                logger.info("message from twitter : -> " + msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("error reading twiiter : ", e);
                client.stop();
            }
            /*something(msg);
            profit();*/
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        final String bootStrapServers = "localhost:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //kafka > 0.11

        //high throughput producer at bit of latency & CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20 ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32kb batch size

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private Client createTwitterClient() {

        //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
}
