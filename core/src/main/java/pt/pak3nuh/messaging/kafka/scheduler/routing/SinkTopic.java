package pt.pak3nuh.messaging.kafka.scheduler.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.ClientMessage;
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
            if (deliverClient) {
                final ClientMessage clientMessage = content.getClientMessage();
                LOGGER.trace("Client message {}", clientMessage);
                producer.send(destination, clientMessage);
            } else {
                LOGGER.trace("Internal message {}", content);
                producer.send(destination, content);
            }

        } catch (Exception ex) {
            SchedulerException schedulerException;
            if (ex instanceof SchedulerException) {
                schedulerException = (SchedulerException) ex;
            } else {
                schedulerException = new SchedulerException("Can't produce message with id " + content.getId(), ex);
            }

            handler.handle(content.getClientMessage(), schedulerException);
            throw schedulerException;
        }
    }
}
