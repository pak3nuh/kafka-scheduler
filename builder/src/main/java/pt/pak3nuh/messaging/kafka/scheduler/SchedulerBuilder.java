package pt.pak3nuh.messaging.kafka.scheduler;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import pt.pak3nuh.messaging.kafka.scheduler.consumer.ConsumerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.dispatcher.InternalThreadDispatcher;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;
import pt.pak3nuh.messaging.kafka.scheduler.producer.ProducerImpl;
import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouterImpl;

import java.time.Instant;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static pt.pak3nuh.messaging.kafka.scheduler.util.Check.checkNotEmpty;
import static pt.pak3nuh.messaging.kafka.scheduler.util.Check.checkNotNull;
import static pt.pak3nuh.messaging.kafka.scheduler.util.Check.checkPositive;

public final class SchedulerBuilder {

    private Set<SchedulerTopic> topics = new HashSet<>();
    private MessageFailureHandler handler = new LoggingFailureHandler();
    private String servers;
    private String appName = "kafka-scheduler";

    public SchedulerBuilder(String servers) {
        this.servers = checkNotEmpty(servers);
    }

    public SchedulerBuilder addScheduleMinutes(int minutes) {
        checkPositive(minutes);
        topics.add(new SchedulerTopic(minutes, SchedulerTopic.Granularity.MINUTES, appName));
        return this;
    }

    public SchedulerBuilder addScheduleHours(int hours) {
        checkPositive(hours);
        topics.add(new SchedulerTopic(hours, SchedulerTopic.Granularity.HOURS, appName));
        return this;
    }

    public SchedulerBuilder failureHandler(MessageFailureHandler handler) {
        this.handler = checkNotNull(handler);
        return this;
    }

    public SchedulerBuilder appName(String appName) {
        this.appName = appName;
        topics = topics.stream()
                .map(topic -> new SchedulerTopic(topic.getHoldValue(), topic.getGranularity(), appName))
                .collect(Collectors.toSet());
        return this;
    }

    private Producer getProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return new ProducerImpl(new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer()));
    }

    public Scheduler build() {
        Producer producer = getProducer();
        TopicRouterImpl router = new TopicRouterImpl(
                checkNotEmpty(topics),
                handler,
                producer
        );
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        long granularity = topics.stream().mapToLong(SchedulerTopic::toSeconds).min().getAsLong();
        RoutingScheduler routingScheduler = new RoutingScheduler(router, granularity);
        InternalThreadDispatcher dispatcher = new InternalThreadDispatcher(
                topics,
                new ConsumerFactory(servers, appName),
                routingScheduler
        );
        return new SchedulerWrapper(producer, dispatcher, servers, routingScheduler);
    }

    private static class SchedulerWrapper implements Scheduler {
        private final Producer producer;
        private final InternalThreadDispatcher dispatcher;
        private final String servers;
        private final RoutingScheduler routingScheduler;

        public SchedulerWrapper(Producer producer, InternalThreadDispatcher dispatcher, String servers, RoutingScheduler routingScheduler) {
            this.producer = producer;
            this.dispatcher = dispatcher;
            this.servers = servers;
            this.routingScheduler = routingScheduler;
        }

        @Override
        public void start() {
            checkServer();
            dispatcher.start();
        }

        private void checkServer() throws SchedulerException {
            Properties properties = new Properties();
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            try (AdminClient client = AdminClient.create(properties)) {
                client.describeCluster().clusterId().get();
            } catch (Exception e) {
                throw new SchedulerException(e);
            }
        }

        @Override
        public void enqueue(Instant deliverAt, ClientMessage message) {
            routingScheduler.enqueue(deliverAt, message);
        }

        @Override
        public long granularityInSeconds() {
            return routingScheduler.granularityInSeconds();
        }

        @Override
        public void close() {
            dispatcher.close();
            producer.close();
        }
    }
}
