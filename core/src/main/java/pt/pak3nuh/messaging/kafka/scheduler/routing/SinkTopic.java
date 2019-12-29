package pt.pak3nuh.messaging.kafka.scheduler.routing;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.MessageFailureHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;
import pt.pak3nuh.messaging.kafka.scheduler.Topic;
import pt.pak3nuh.messaging.kafka.scheduler.annotation.VisibleForTesting;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;

final class SinkTopic implements Topic {
    private final Producer producer;
    private final String destination;
    private final MessageFailureHandler handler;

    public SinkTopic(Producer producer, String destination, MessageFailureHandler handler) {
        this.producer = producer;
        this.destination = destination;
        this.handler = handler;
    }

    @VisibleForTesting
    String getDestination() {
        return destination;
    }

    @Override
    public void send(InternalMessage content) {
        try {
            producer.send(destination, content);
        } catch (Exception ex) {
            handler.handle(content.getClientMessage(), new SchedulerException(ex));
        }
    }
}
