package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.util.Check;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

final class ConsumerImpl implements Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerImpl.class);
    private final org.apache.kafka.clients.consumer.Consumer<String, InternalMessage> consumer;
    private final Duration pollTimeout;
    private final Map<TopicPartition, PausedTopic> pausedPartitionsTimeout = new HashMap<>();

    ConsumerImpl(org.apache.kafka.clients.consumer.Consumer<String, InternalMessage> consumer,
                 Duration pollTimeout) {
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public Iterable<Record> poll() {
        maybeResumePartitions();

        LOGGER.debug("Polling remote data with timeout {}", pollTimeout);
        ConsumerRecords<String, InternalMessage> consumerRecords = consumer.poll(pollTimeout);
        if (consumerRecords.isEmpty()) {
            LOGGER.trace("No records to return");
            return Collections.emptyList();
        }

        LOGGER.trace("Returning records");
        return StreamSupport.stream(consumerRecords.spliterator(), false)
                .map(this::toRecord)
                .collect(toList());
    }

    private void maybeResumePartitions() {
        /*
        Kafka consumer maintains the subscription state even if the partitions is no longer assigned.
        That means we only want to preform operations on assigned partitions or else the pause/resume will fail.
        We can hold the paused timeout for a partition as long as needed, but only operate on it when is assigned
        */
        Instant now = Instant.now();
        Set<TopicPartition> assignment = consumer.assignment();
        LOGGER.trace("Consumer current assignment {}", assignment);
        assignment.forEach(assigned -> {
            PausedTopic pausedTopic = pausedPartitionsTimeout.get(assigned);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Assigned partition {} on position {} has pause timeout {}", assigned, consumer.position(assigned), pausedTopic);
            }
            if (null != pausedTopic) {
                if (pausedTopic.hasTimedOut(now)) {
                    LOGGER.debug("Resuming partition {} and preform seek {} due to pause timeout", assigned, pausedTopic.offset);
                    consumer.resume(singleton(assigned));
                    consumer.seek(assigned, new OffsetAndMetadata(pausedTopic.offset));
                    pausedPartitionsTimeout.remove(assigned);
                }
            }
        });
    }

    private Record toRecord(ConsumerRecord<String, InternalMessage> record) {
        return new R(record.topic(), record.partition(), record.offset(), record.value());
    }

    @Override
    public void commit(Record record) {
        Check.check(record instanceof R, "Unknown record type");
        R r = (R) record;
        LOGGER.debug("Committing record {}", r);
        TopicPartition topicPartition = new TopicPartition(r.getTopic(), r.getPartition());
        consumer.commitSync(Collections.singletonMap(
                topicPartition,
                new OffsetAndMetadata(r.getOffset())));
    }

    @Override
    public void pause(Record record, Instant until) {
        Check.check(record instanceof R, "Unknown record type");
        R r = (R) record;
        LOGGER.debug("Pausing on record {} until {}", r, until);
        TopicPartition topicPartition = new TopicPartition(r.topic, r.partition);
        PausedTopic pausedTopic = new PausedTopic(r.offset, until);
        // should save the minimum offset (and instant by adjacency)
        pausedPartitionsTimeout.compute(topicPartition, (key, stored) -> {
            if (stored == null) {
                LOGGER.trace("Pausing consumer partition {}", key);
                consumer.pause(singleton(key));
                return pausedTopic;
            }
            PausedTopic minOffset = pausedTopic.min(stored);
            LOGGER.trace("Partition already paused, keeping offset {}", minOffset);
            return minOffset;
        });
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Value
    private static class R implements Record {
        private final String topic;
        private final int partition;
        private final long offset;
        private final InternalMessage message;
    }

    @Value
    private static class PausedTopic {
        private final long offset;
        private final Instant pausedUntil;

        public PausedTopic min(PausedTopic other) {
            if (offset <= other.offset) return this;
            return other;
        }

        public boolean hasTimedOut(Instant instant) {
            return pausedUntil.compareTo(instant) < 0;
        }
    }
}
