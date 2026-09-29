package work.jeong.murry.example.kafka;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;

import java.lang.reflect.Type;

public class TaskSerde extends Serdes {

  public TaskSerde() {
    super();
  }

  private static Gson GSON = new Gson();

  public static byte[] serialize(Task task) {
    String data = task.getClass().getName() + ";" + GSON.toJson(task);
    return data.getBytes();
  }

  public static Task deserialize(byte[] data) throws ClassNotFoundException {
    String[] parts = new String(data).split(";", 2);
    Type type = Class.forName(parts[0]);
    return GSON.fromJson(parts[1], type);
  }

}
