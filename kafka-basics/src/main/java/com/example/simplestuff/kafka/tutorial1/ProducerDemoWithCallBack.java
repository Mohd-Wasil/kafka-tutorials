package com.example.simplestuff.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

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
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("states", "UP, MP, RJ, KL, AP, TL, CH, DL :-> " + Integer.toString(i));
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a recrod is sent - success / exception
                        if (e == null) {
                            //record was successfully sent
                            logger.info("recieved metadata : \n" +
                                    " Topic : " + recordMetadata.topic() + " \n"
                                    + "Partition : " + recordMetadata.partition() + " \n"
                                    + "OffSet : " + recordMetadata.offset() + " \n"
                                    + "TimeStamp : " + recordMetadata.timestamp() + " \n");
                        } else {
                            logger.error("exeption while sending message : ", e);
                        }
                    }
                });
                //flush data
                producer.flush();

                System.out.println("message sent..");
            }
            //flush & close producer
            producer.close();
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}
