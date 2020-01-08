package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;

/**
 * <p>A message delivery system with scheduling capabilities.</p>
 * <p>The delivery semantics are just an estimation and not an exact promise. The actual delivery time depends
 * on external factors like latency or service pressure.</p>
 * <p>The consumer on the {@link ClientMessage#getDestination()} topic will receive a message with a generated
 * {@link String} id and the array of bytes in the {@link ClientMessage#getContent()}.</p>
 */
public interface Scheduler extends AutoCloseable {
    /**
     * Starts the scheduler.
     */
    void start();
    /**
     * <p>Enqueues a message for delivery starting at <code>instant</code> and never sooner.</p>
     * @param deliverAt The instant for message delivery
     * @param message The message to deliver
     * @see #granularityInSeconds()
     */
    void enqueue(Instant deliverAt, ClientMessage message);

    /**
     * <p>Returns in seconds the finer granularity available on the scheduler.</p>
     * @return the finer available granularity between schedules.
     */
    long granularityInSeconds();

    /**
     * Closes the scheduler.
     */
    @Override
    void close();
}
