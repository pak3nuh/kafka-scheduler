package pt.pak3nuh.messaging.kafka.scheduler;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.mandas.kafka.KafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class StressTest {

    public static final Random RANDOM = new Random();
    private static final KafkaCluster cluster = MemoryClusterFactory.create();
    private static final Logger LOGGER = LoggerFactory.getLogger(StressTest.class);

    public static void main(String[] args) throws Throwable {
        withMemoryKafka();
//        withLiveCluster();
    }

    private static void withLiveCluster() throws Throwable {
        final Properties props = new Properties();
        final String brokers = "localhost:9092";
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        final AdminClient adminClient = AdminClient.create(props);
//        deleteAll(adminClient);
        final Set<String> existingTopics = adminClient.listTopics().names().get();
        try {
            runTest(brokers);
        } finally {
            final Set<String> currentTopics = adminClient.listTopics().names().get();
            final Set<String> toRemove = currentTopics.stream().filter(it -> !existingTopics.contains(it)).collect(Collectors.toSet());
            adminClient.deleteTopics(toRemove).all().get();
            adminClient.close();
        }
    }

    private static void deleteAll(AdminClient client) throws ExecutionException, InterruptedException {
        final Set<String> existingTopics = client.listTopics().names().get();
        client.deleteTopics(existingTopics).all().get();
    }

    private static void withMemoryKafka() throws Throwable {
        String brokers = cluster.brokers();
        cluster.start();
        try {
            runTest(brokers);
        } finally {
            cluster.shutdown();
        }
    }

    private static void runTest(String brokers) throws Throwable {
        SchedulerBuilder builder = new SchedulerBuilder(brokers).appName("stress-test-")
                .addScheduleMinutes(1)
                .addScheduleMinutes(3)
                .addScheduleMinutes(5)
                .addScheduleMinutes(10);

        final int numMessages = 1_000_000;
        final int testDurationMinutes = 10;

        try (Scheduler scheduler = builder.build()) {
            final Duration deliveryDelta = Duration.ofSeconds(scheduler.granularityInSeconds() + 10);
            final Instant maxMessageDelivery = Instant.now().plus(Duration.ofMinutes(testDurationMinutes));
            final String destinationTopic = "stress-test-destination";

            try (MessageWaiter waiter = new MessageWaiter(destinationTopic, brokers, deliveryDelta, scheduler, numMessages)) {
                scheduler.start();
                for (int i = 0; i < numMessages; i++) {
                    waiter.enqueueMessage(String.valueOf(i), randomArrival(maxMessageDelivery));
                }

                final MessageWaiter.Result result = waiter.waitForTermination(maxMessageDelivery.plus(deliveryDelta));
                if (result.isSuccess()) {
                    LOGGER.info("All messages delivered on time");
                } else {
                    LOGGER.error("Not all data was delivered as expected {}", result);
                    throw new IllegalStateException("Result was not successful");
                }
            }
        }
    }

    private static Instant randomArrival(Instant timeout) {
        final Instant now = Instant.now();
        final int seconds = (int) now.until(timeout, ChronoUnit.SECONDS);
        final Instant arrival = timeout.minusSeconds(RANDOM.nextInt(seconds));
        if (arrival.isBefore(now) || arrival.isAfter(timeout)) {
            throw new IllegalStateException("Wrong random arrival");
        }
        return arrival;
    }
}
