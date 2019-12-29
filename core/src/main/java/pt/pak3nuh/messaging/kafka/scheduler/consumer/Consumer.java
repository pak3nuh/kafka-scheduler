package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

import java.time.Instant;

/**
 * <p>Consumes the records over a kafka topic and hides partition pausing mechanics.</p>
 * <p>This consumer is designed to work with the default threading model for kafka consumer, meaning not concurrent.</p>
 * <p>All the operations submitted to kafka are synchronous.</p>
 */
public interface Consumer extends AutoCloseable {
    /**
     * Gets an iterable of records
     */
    Iterable<Record> poll();

    /**
     * Submits the offsets to kafka and marks the record as processed.
     */
    void commit(Record record);

    /**
     * <p>Pauses the source partition of the record until the instant has passed on the record's offset.</p>
     * <p>The next {@link #poll()} after {@code until} has passed, the {@code record} will be returned again.</p>
     * @param record The source record
     * @param until The time until the partition should be paused
     */
    void pause(Record record, Instant until);

    @Override
    void close();

    interface Record {
        InternalMessage getMessage();
    }
}
