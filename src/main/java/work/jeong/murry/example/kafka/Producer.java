package work.jeong.murry.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static work.jeong.murry.example.kafka.KafkaMessageUtils.serialize;

public class Producer {

  public static void main(String[] args) {
    String topic = "test-topic";
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", "localhost:9092");
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    executorService.submit(() -> {
      while (true) {
        System.out.println("Write");
        MessageContext messageContext = new MessageContext("title", "body");
        Task task = new MessageTask(messageContext);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, serialize(task));
        kafkaProducer.send(producerRecord);
        Thread.sleep(5000);
      }
    });
  }

}
