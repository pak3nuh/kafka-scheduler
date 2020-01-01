package pt.pak3nuh.messaging.kafka.scheduler;

import org.apache.kafka.clients.consumer.Consumer;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class MessageEnqueueingTest {

    private static final String TOPIC = "destination";
    private static final KafkaCluster cluster = KafkaCluster.builder()
            .withZookeeper("127.0.0.1", 10000, 10001)
            .withBroker(1, "127.0.0.1", 10002, 10003)
            .build();

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
    void shouldDeliverAtSpecifiedTime() {
        SchedulerBuilder builder = new SchedulerBuilder(cluster.brokers()).addScheduleMinutes(1);

        try(Scheduler scheduler = builder.build()) {
            scheduler.start();
            Instant timeToDeliver = Instant.now().plusSeconds(60);
            String payload = "payload";
            scheduler.enqueue(timeToDeliver, new ClientMessage("blah", TOPIC, payload.getBytes()));
            Assertions.assertTimeout(Duration.ofMinutes(2), () -> waitForResult(payload));
        }
    }

    private void waitForResult(String payload) {
        ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(new Properties(), deserializer, deserializer)) {
            consumer.subscribe(Collections.singleton(TOPIC));
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                if (!records.isEmpty()) {
                    ConsumerRecord<byte[], byte[]> record = records.iterator().next();
                    byte[] bytes = record.value();
                    String result = new String(bytes);
                    Assertions.assertEquals(payload, result);
                    break;
                }
            }
        }
    }
}
