package pt.pak3nuh.messaging.kafka.scheduler.producer;

import pt.pak3nuh.messaging.kafka.scheduler.ClientMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

public interface Producer extends AutoCloseable {
    void send(String topic, InternalMessage content);

    void send(String topic, ClientMessage content);

    @Override
    void close();
}
