package com.example.simplestuff.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        try {
            System.out.println("hello sample class");
            final String bootStrapServers = "localhost:9092";
            //create producer properties
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            //create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            //create a Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("states", "UP, MP, RJ, KL, AP, TL, CH, DL");
            //send data
            producer.send(producerRecord);
            //flush data
            producer.flush();
            //flush & close producer
            producer.close();
            System.out.println("message sent..");
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}
