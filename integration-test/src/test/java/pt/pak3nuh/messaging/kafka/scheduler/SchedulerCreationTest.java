package pt.pak3nuh.messaging.kafka.scheduler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mandas.kafka.KafkaCluster;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class SchedulerCreationTest {

    private static final KafkaCluster cluster = MemoryClusterFactory.create();

    @BeforeAll
    static void beforeAll() {
        cluster.start();
    }

    @AfterAll
    static void afterAll() {
        cluster.shutdown();
    }

    @Test
    void shouldCreateScheduler() {
        Scheduler scheduler = new SchedulerBuilder(cluster.brokers())
                .addScheduleMinutes(1)
                .addScheduleHours(1)
                .build();

        scheduler.start();
        scheduler.close();
    }

}
