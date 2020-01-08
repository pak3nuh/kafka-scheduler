package pt.pak3nuh.messaging.kafka.scheduler;

public interface InternalMessageHandler {
    /**
     * When a message is received on an internal topic and should be routed to the next topic.
     * @param message The message to route.
     */
    void handle(InternalMessage message);
}
