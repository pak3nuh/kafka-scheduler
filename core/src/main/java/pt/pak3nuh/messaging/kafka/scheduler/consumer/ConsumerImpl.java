package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.singleton;

final class ConsumerImpl implements Consumer {

    private final org.apache.kafka.clients.consumer.Consumer<String, InternalMessage> consumer;
    private final Duration pollTimeout;
    private final Map<TopicPartition, PausedTopic> pausedPartitions = new HashMap<>();

    ConsumerImpl(org.apache.kafka.clients.consumer.Consumer<String, InternalMessage> consumer,
                 Duration pollTimeout) {
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public Iterable<Record> poll() {
        maybeResumePartitions();

        ConsumerRecords<String, InternalMessage> consumerRecords = consumer.poll(pollTimeout);
        if (consumerRecords.isEmpty())
            return Collections.emptyList();

        return StreamSupport.stream(consumerRecords.spliterator(), false)
                .map(this::toRecord)
                .collect(Collectors.toList());
    }

    private void maybeResumePartitions() {
        Instant now = Instant.now();
        pausedPartitions.entrySet().removeIf(entry -> {
            PausedTopic pausedTopic = entry.getValue();
            if (min(now, pausedTopic.pausedUntil) != now) {
                consumer.resume(singleton(entry.getKey()));
                consumer.seek(entry.getKey(), new OffsetAndMetadata(pausedTopic.offset + 1));
                return true;
            }
            return false;
        });
    }

    private Record toRecord(ConsumerRecord<String, InternalMessage> record) {
        return new R(record.topic(), record.partition(), record.offset(), record.value());
    }

    @Override
    public void commit(Record record) {
        R r = ((R) record);
        TopicPartition topicPartition = new TopicPartition(r.getTopic(), r.getPartition());
        consumer.commitSync(Collections.singletonMap(
                topicPartition,
                new OffsetAndMetadata(r.getOffset())));
    }

    @Override
    public void pause(Record record, Instant until) {
        R r = (R) record;
        TopicPartition topicPartition = new TopicPartition(r.topic, r.partition);
        PausedTopic pausedTopic = new PausedTopic(r.offset, until);
        // should save the minimum offset (and instant by adjacency)
        pausedPartitions.compute(topicPartition, (key, stored) -> {
            if (stored == null) {
                consumer.pause(singleton(key));
                return pausedTopic;
            }
            return pausedTopic.min(stored);
        });
    }

    @Override
    public void close() {
        consumer.close();
    }

    // todo see Manifold or Lombok for extension methods
    // https://github.com/manifold-systems/manifold/tree/master/manifold-deps-parent/manifold-ext
    private Instant min(Instant stored, Instant until) {
        return stored.compareTo(until) <= 0 ? stored : until;
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
            if(offset <= other.offset) return this;
            return other;
        }
    }
}
