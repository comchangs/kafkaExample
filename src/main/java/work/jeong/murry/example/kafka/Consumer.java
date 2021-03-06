package work.jeong.murry.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static work.jeong.murry.example.kafka.KafkaMessageUtils.deserialize;

public class Consumer {

  public static void main(String[] args) {
    String topic = "test-topic";
    ExecutorService executorService = Executors.newCachedThreadPool();
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("group.id", "groupId");
    consumerProps.put("client.id", "clientId");
    consumerProps.put("enable.auto.commit", "false");
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);

    kafkaConsumer.subscribe(Collections.singletonList(topic));
    executorService.submit(() -> {
      while (true) {
        System.out.println("poll");
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
        System.out.println("polled: " + consumerRecords.count());
        for (ConsumerRecord record : consumerRecords) {
          executorService.submit(deserialize((String) record.value()));
          System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
      }
    });
  }

}
