package com.atguigu.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties config = new Properties();
        config.load(MyProducer.class.getClassLoader().getResourceAsStream("producer.properties"));

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(config);

        for (int i = 90; i < 95; i++) {
            Future<RecordMetadata> metadataFuture = producer.send(
                    new ProducerRecord<>(
                            "b",
                            "key--" + i,
                            "AAAAAAAAAAAAAValue-" + i));
//            Thread.sleep(1);

        }
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> metadataFuture = producer.send(
                    new ProducerRecord<String, String>("first", "test-"+i,"hello-key-"+i),
                    new Callback() {

                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (recordMetadata != null) {
                                System.out.println(recordMetadata);
                            } else {
                                e.printStackTrace();
                            }
                        }
                    });

            System.out.println("第" + i + "条消息发送。");
            RecordMetadata recordMetadata = metadataFuture.get();
        }

        producer.close();
    }
}
