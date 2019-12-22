package pt.pak3nuh.messaging.kafka.scheduler;

public interface Topic {
    void send(InternalMessage content);
}
