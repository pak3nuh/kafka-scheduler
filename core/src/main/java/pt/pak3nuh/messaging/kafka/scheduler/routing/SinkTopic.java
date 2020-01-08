package pt.pak3nuh.messaging.kafka.scheduler.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.MessageFailureHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;
import pt.pak3nuh.messaging.kafka.scheduler.Topic;
import pt.pak3nuh.messaging.kafka.scheduler.annotation.VisibleForTesting;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;

final class SinkTopic implements Topic {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkTopic.class);
    private final Producer producer;
    private final String destination;
    private final MessageFailureHandler handler;
    private final boolean deliverClient;

    public SinkTopic(Producer producer, String destination, MessageFailureHandler handler, boolean deliverClient) {
        this.producer = producer;
        this.destination = destination;
        this.handler = handler;
        this.deliverClient = deliverClient;
    }

    @VisibleForTesting
    String getDestination() {
        return destination;
    }

    @Override
    public void send(InternalMessage content) {
        try {
            LOGGER.debug("Sending message with id {} to topic {}", content.getId(), destination);
            LOGGER.trace("Message content {}", content);
            if(deliverClient) {
                producer.send(destination, content.getClientMessage());
            } else {
                producer.send(destination, content);
            }

        } catch (Exception ex) {
            handler.handle(content.getClientMessage(), new SchedulerException(ex));
        }
    }
}
