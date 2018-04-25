package work.jeong.murry.example.kafka;

import com.google.gson.Gson;

import java.lang.reflect.Type;

public class KafkaMessageUtils {

  private static Gson GSON = new Gson();

  public static String serialize(Task task) {
    return task.getClass().getName() + ";" + GSON.toJson(task);
  }

  public static Task deserialize(String message) throws ClassNotFoundException {
    String[] parts = message.split(";", 2);
    Type type = Class.forName(parts[0]);
    return GSON.fromJson(parts[1], type);
  }

}
