package com.atguigu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "test2");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");//是否自动提交

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        kafkaConsumer.subscribe(Collections.singleton("b"));

        ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofSeconds(10));

        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record);
        }

        //消费完数据，提交offset
//        kafkaConsumer.commitSync();
        kafkaConsumer.commitAsync((offsets, e) -> {
            if (e == null) {
                System.out.println(offsets);
            } else {
                e.printStackTrace();
            }

        });

        kafkaConsumer.close();
    }
}
