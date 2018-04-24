package work.jeong.murry.example.kafka;

public class MessageContext {

  private final String title;

  private final String body;

  public MessageContext(String title, String body) {
    this.title = title;
    this.body = body;
  }

  public String getTitle() {
    return title;
  }

  public String getBody() {
    return body;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("MessageContext{");
    sb.append("title='").append(title).append('\'');
    sb.append(", body='").append(body).append('\'');
    sb.append('}');
    return sb.toString();
  }

}
