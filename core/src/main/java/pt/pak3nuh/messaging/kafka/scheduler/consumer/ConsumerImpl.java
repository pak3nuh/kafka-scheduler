package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final class ConsumerImpl implements Consumer {

    private final org.apache.kafka.clients.consumer.Consumer<String, InternalMessage> consumer;
    private final Duration pollTimeout;

    ConsumerImpl(org.apache.kafka.clients.consumer.Consumer<String, InternalMessage> consumer,
                 Duration pollTimeout) {
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public Iterable<Consumer.Record> poll() {
        ConsumerRecords<String, InternalMessage> consumerRecords = consumer.poll(pollTimeout);
        if (consumerRecords.isEmpty())
            return Collections.emptyList();

        return StreamSupport.stream(consumerRecords.spliterator(), false)
                .map(this::toRecord)
                .collect(Collectors.toList());
    }

    @Override
    public void commit(Record record) {
        consumer.commitSync(Collections.singletonMap(
                new TopicPartition(record.getTopic(), record.getPartition()),
                new OffsetAndMetadata(record.getOffset())));
    }

    private Record toRecord(ConsumerRecord<String, InternalMessage> record) {
        return new R(record.topic(), record.partition(), record.offset(), record.value());
    }

    @Data
    private static class R implements Record {
        private final String topic;
        private final int partition;
        private final long offset;
        private final InternalMessage message;
    }
}
