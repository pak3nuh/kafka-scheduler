package pt.pak3nuh.messaging.kafka.scheduler;

/**
 * Represents a topic with message production capabilities
 */
public interface Topic {
    void send(InternalMessage content);
}
