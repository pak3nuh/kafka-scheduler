package pt.pak3nuh.messaging.kafka.scheduler;

public interface InternalMessageHandler {
    void handle(InternalMessage message);
}
