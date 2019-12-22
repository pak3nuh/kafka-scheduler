package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;

public interface Scheduler {
    /**
     * <p>Enqueues a message for delivery starting at <code>instant</code> and never sooner.</p>
     * <p>Actual delivery guarantees depend on the granularity of the schedule topics</p>
     * @param instant The instant for message delivery
     * @param message The message to deliver
     */
    void enqueue(Instant instant, ClientMessage message);
}
