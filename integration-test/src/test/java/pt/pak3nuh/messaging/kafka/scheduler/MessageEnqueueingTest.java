package pt.pak3nuh.messaging.kafka.scheduler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mandas.kafka.KafkaCluster;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class MessageEnqueueingTest {

    private static final String TOPIC = "destination";
    private static final KafkaCluster cluster = MemoryClusterFactory.create();

    @BeforeAll
    static void beforeAll() {
        cluster.start();
        cluster.createTopic(TOPIC, 10);
    }

    @AfterAll
    static void afterAll() {
        cluster.shutdown();
    }

    @Test
    void shouldDeliverAtSpecifiedTime() throws TimeoutException {
        SchedulerBuilder builder = new SchedulerBuilder(cluster.brokers()).addScheduleMinutes(1);

        try(Scheduler scheduler = builder.build()) {
            scheduler.start();
            Instant timeToDeliver = Instant.now().plusSeconds(60);
            String payload = "payload";
            scheduler.enqueue(timeToDeliver, new ClientMessage("blah".getBytes(), TOPIC, payload.getBytes()));
            waitForResult(payload, timeToDeliver, scheduler.granularityInSeconds());
        }
    }

    private void waitForResult(String payload, Instant estimatedTime, long delay) throws TimeoutException {
        Instant timeout = estimatedTime.plusSeconds(delay);
        ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "some-id");
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(properties, deserializer, deserializer)) {
            consumer.subscribe(Collections.singleton(TOPIC));
            while (Instant.now().compareTo(timeout) < 0) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                if (!records.isEmpty()) {
                    ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                    String actualPayload = new String(record.value());
                    Assertions.assertEquals(payload, actualPayload);
                    return;
                }
            }
        }
        throw new TimeoutException();
    }
}
