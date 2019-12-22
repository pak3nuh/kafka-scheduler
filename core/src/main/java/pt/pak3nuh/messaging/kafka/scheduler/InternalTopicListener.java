package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.consumer.Consumer;

public class InternalTopicListener {
    private final InternalMessageHandler handler;
    private final Consumer consumer;
    private final long delayInSeconds;

    public InternalTopicListener(InternalMessageHandler handler, Consumer consumer, long delayInSeconds) {
        this.handler = handler;
        this.consumer = consumer;
        this.delayInSeconds = delayInSeconds;
    }


}
