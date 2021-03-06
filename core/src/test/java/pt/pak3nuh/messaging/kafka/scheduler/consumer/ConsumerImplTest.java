package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Iterables;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory.create;

class ConsumerImplTest {

    private static final SchedulerTopic SCHEDULER_TOPIC = new SchedulerTopic(1, SchedulerTopic.Granularity.MINUTES, "app");
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(SCHEDULER_TOPIC.topicName(), 1);
    private final MockConsumer<String, InternalMessage> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final ConsumerImpl consumer = new ConsumerImpl(mock, Duration.ZERO);
    private long offset;

    {
        mock.assign(singleton(TOPIC_PARTITION));
        mock.seek(TOPIC_PARTITION, 0);
    }

    @Test
    void shouldReturnRecordsAndAdvanceLocalOffset() {
        addRecord();
        addRecord();

        Iterable<Consumer.Record> records = consumer.poll();

        long numberOfRecords = StreamSupport.stream(records.spliterator(), false).count();
        assertEquals(2, numberOfRecords);
        assertEquals(offset, mock.position(TOPIC_PARTITION));
    }

    @Test
    void shouldCommitOffset() {
        long currentOffset = offset;
        addRecord();

        Iterable<Consumer.Record> records = consumer.poll();
        Consumer.Record firstOf = Iterables.firstOf(records);
        consumer.commit(firstOf);

        Map<TopicPartition, OffsetAndMetadata> committed = mock.committed(singleton(TOPIC_PARTITION));
        assertEquals(1, committed.size());
        assertEquals(currentOffset + 1, committed.get(TOPIC_PARTITION).offset());
    }

    @Test
    void shouldPausePartitions() {
        addRecord();

        Iterable<Consumer.Record> records = consumer.poll();
        Consumer.Record firstOf = Iterables.firstOf(records);
        consumer.pause(firstOf, Instant.now().plusSeconds(60));

        assertTrue(mock.paused().contains(TOPIC_PARTITION));
    }

    @Test
    void shouldResumePartitionsWithTheCorrectOffset() {
        long lowestOffset = offset;
        addRecord();
        addRecord();

        // should pause on the record with the lowest offset
        for (Consumer.Record record : consumer.poll()) {
            consumer.pause(record, Instant.EPOCH);
        }

        assertTrue(mock.paused().contains(TOPIC_PARTITION));
        assertEquals(offset, mock.position(TOPIC_PARTITION));

        // empty iterable because of implementation details of MockConsumer
        consumer.poll();
        // no partitions should be paused and the offset must be 0 again
        assertFalse(mock.paused().contains(TOPIC_PARTITION));
        assertEquals(lowestOffset, mock.position(TOPIC_PARTITION));
    }

    @Test
    void shouldNotHandleUnassignedPartitions() {
        addRecord();
        Consumer.Record record = consumer.poll().iterator().next();
        consumer.pause(record, Instant.EPOCH);

        mock.unsubscribe();
        consumer.poll();
        assertTrue(mock.paused().contains(TOPIC_PARTITION));
    }

    @Test
    void shouldHandleRebalancesGracefully() {
        addRecord(5);

        // can't process record, pause partition
        Consumer.Record record = consumer.poll().iterator().next();
        consumer.pause(record, Instant.EPOCH);
        assertTrue(mock.paused().contains(TOPIC_PARTITION));

        mock.unsubscribe();

        // clears unsubsribed paused partitions
        consumer.poll();
        assertTrue(mock.paused().contains(TOPIC_PARTITION));

        mock.assign(singleton(TOPIC_PARTITION));
        consumer.poll();
        assertTrue(mock.paused().isEmpty());
    }

    @Test
    void shouldMaintainKafkaSubscriptionStateAfterReassignment() {
        mock.pause(singleton(TOPIC_PARTITION));

        mock.unsubscribe();
        assertTrue(mock.paused().contains(TOPIC_PARTITION));

        mock.assign(singleton(TOPIC_PARTITION));
        assertTrue(mock.paused().contains(TOPIC_PARTITION));

        mock.unsubscribe();
        assertTrue(mock.paused().contains(TOPIC_PARTITION));

        mock.subscribe(singleton(TOPIC_PARTITION.topic()));
        assertTrue(mock.paused().contains(TOPIC_PARTITION));
    }

    @Test
    void shouldPositionCorrectlyOnCommit() {
        // this avoids receiving duplicate records between two poll calls with error
        addRecord();
        final Iterator<Consumer.Record> firstPoll = consumer.poll().iterator();
        consumer.commit(firstPoll.next());
        assertEquals(offset, mock.position(TOPIC_PARTITION));
        assertEquals(offset, mock.committed(singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION).offset());
    }

    private void addRecord() {
        addRecord(0);
    }

    private void addRecord(int minutes) {
        mock.addRecord(new ConsumerRecord<>(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), offset++, "key", create(minutes, 0)));
    }

}