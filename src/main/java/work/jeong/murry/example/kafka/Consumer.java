package work.jeong.murry.example.kafka;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        for (ConsumerRecord record: consumerRecords) {
          Gson gson = new Gson();
          Type type = Class.forName((String) record.key());
          System.out.println(type.getTypeName());
          Task task = gson.fromJson((String) record.value(), type);
          executorService.submit(task);
          System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
      }
    });
  }

}
