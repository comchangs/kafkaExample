package work.jeong.murry.example.kafka;

public class MessageTask implements Task {

  private final MessageContext messageContext;

  public MessageTask(MessageContext messageContext) {
    this.messageContext = messageContext;
  }

  @Override
  public void run() {
    System.out.println("Processing: " + messageContext);
  }
}
