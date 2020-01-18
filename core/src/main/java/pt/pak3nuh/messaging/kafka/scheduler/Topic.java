package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.producer.ProducerClosedException;

/**
 * Represents a topic with message production capabilities
 */
public interface Topic {
    /**
     * Sends a message to an intermediate topic or the final topic.
     * @param content The message to enqueue
     * @throws SchedulerException If the message couldn't be sent to the destination topic.
     * <p>If the producer has been closed, a {@link ProducerClosedException} is thrown, all other exceptions
     * are wrapped into {@link SchedulerException}.</p>
     */
    void send(InternalMessage content) throws SchedulerException;
}
