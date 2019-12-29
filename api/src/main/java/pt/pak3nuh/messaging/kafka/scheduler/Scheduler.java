package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;

/**
 * <p>A message delivery system with scheduling capabilities.</p>
 * <p>The delivery semantics are just an estimation and not an exact promise. The actual delivery time depends
 * on external factors like latency or service pressure and the least amount of granularity provided.</p>
 */
public interface Scheduler extends AutoCloseable {
    /**
     * Starts the scheduler.
     */
    void start();
    /**
     * <p>Enqueues a message for delivery starting at <code>instant</code> and never sooner.</p>
     * <p>Actual delivery guarantees depend on the granularity of the schedule topics</p>
     * @param deliverAt The instant for message delivery
     * @param message The message to deliver
     * @see #granularityInSeconds()
     */
    void enqueue(Instant deliverAt, ClientMessage message);

    /**
     * <p>Returns in seconds the finer granularity available on the scheduler. This value will tell the maximum amount
     * of delay estimated between the delivery time requested and the actual delivery.</p>
     * <p>The estimation is excluding factors like network latency or service pressure.</p>
     * @return the finer available granularity between schedules.
     */
    long granularityInSeconds();

    /**
     * Closes the scheduler.
     */
    @Override
    void close();
}
