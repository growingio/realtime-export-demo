package io.growing.export.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * GrowingIO realtime-export demo
 */
public class JavaDemo {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("usage: JavaDemo <groupId> <username> <password> <topics>");
            return;
        }
        String groupId = args[0];
        Properties properties = KafkaProperties.saslProperties(groupId, args[1], args[2]);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(args[3].split(",")));

        int numMessageToConsume = 1000;
        int messageRemaining = numMessageToConsume;
        while (messageRemaining > 0) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(groupId + " received message : from partition " + record.partition() + ", (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
            messageRemaining -= records.count();
        }

        System.out.println(groupId + " finished reading " + numMessageToConsume + " messages");
    }
}
