package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer {
    public static void main(String[] args) throws IOException {

        final String SENSOR0101 = "/sample-stream:sensor1-region1";

        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }
      
        try {
            FileInputStream fstream = new FileInputStream("/mapr/demo.mapr.com/user/mapr/ethylene_methane_2.csv");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;
            Integer i = 0;
            // Read File Line By Line
            while ((strLine = br.readLine()) != null)   {
                i++;
                producer.send(new ProducerRecord<String, String>(
                        SENSOR0101,
                        strLine));

                // every so often send to a different topic
                if (i % 1000 == 0) {
                    producer.flush();
                    System.out.println("Sent msg number " + i);
                }

                Thread.sleep(10);
            }

            // Close the input stream
            br.close();

        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
}
