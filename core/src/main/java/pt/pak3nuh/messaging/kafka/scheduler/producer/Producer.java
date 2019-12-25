package pt.pak3nuh.messaging.kafka.scheduler.producer;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

public interface Producer extends AutoCloseable {
    void send(String topic, InternalMessage content);

    @Override
    void close();
}
