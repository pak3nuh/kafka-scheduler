package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

import java.time.Instant;

/**
 * <p>Consumes the records over a kafka topic and hides pausing and seeking mechanics.</p>
 * <p>This consumer is designed to work with the default threading model for kafka consumer, meaning not concurrent.
 * It should also be used like a log, meaning you process a record, mark it as processed and move to the next.</p>
 * <p>All the operations submitted to kafka are synchronous.</p>
 */
public interface Consumer extends AutoCloseable {
    /**
     * Gets an iterable of records.
     * <p>If it is the first time, then it will delegate to the kafka consumer settings, if its not the first time
     * then the next records should be the ones after the last {@link #commit(Record)}.</p>
     */
    Iterable<Record> poll();

    /**
     * <p>Submits the offsets to kafka, marking the record as processed, and ensures the next {@link #poll()} call
     * will return only newer records than {@code record}.</p>
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
