package pt.pak3nuh.messaging.kafka.scheduler;

public interface MessageFailureHandler {
    void handle(ClientMessage message, SchedulerException cause);
}
