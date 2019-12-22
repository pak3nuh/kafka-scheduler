package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

public class DispatcherTopicConfig {
    private final String topicName;
    private final int delaySeconds;

    public DispatcherTopicConfig(String topicName, int delaySeconds) {
        this.topicName = topicName;
        this.delaySeconds = delaySeconds;
    }
}
