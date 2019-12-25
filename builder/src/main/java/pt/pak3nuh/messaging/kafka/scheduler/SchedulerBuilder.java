package pt.pak3nuh.messaging.kafka.scheduler;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import pt.pak3nuh.messaging.kafka.scheduler.consumer.ConsumerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.data.InternalMessageSerde;
import pt.pak3nuh.messaging.kafka.scheduler.dispatcher.InternalThreadDispatcher;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;
import pt.pak3nuh.messaging.kafka.scheduler.producer.ProducerImpl;
import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouterImpl;

import java.time.Instant;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static pt.pak3nuh.messaging.kafka.scheduler.util.Check.*;

public final class SchedulerBuilder {

    private final Set<SchedulerTopic> topics = new HashSet<>();
    private MessageFailureHandler handler = new LoggingFailureHandler();
    private Producer producer;
    private String servers;

    public SchedulerBuilder(String servers) {
        this.servers = checkNotEmpty(servers);
    }

    public SchedulerBuilder addScheduleMinutes(int minutes) {
        checkPositive(minutes);
        topics.add(new SchedulerTopic(minutes, SchedulerTopic.Granularity.MINUTES));
        return this;
    }

    public SchedulerBuilder addScheduleHours(int hours) {
        checkPositive(hours);
        topics.add(new SchedulerTopic(hours, SchedulerTopic.Granularity.HOURS));
        return this;
    }

    public SchedulerBuilder failureHandler(MessageFailureHandler handler) {
        this.handler = checkNotNull(handler);
        return this;
    }

    public SchedulerBuilder producer(Producer producer) {
        this.producer = checkNotNull(producer);
        return this;
    }

    private Producer getProducer() {
        if(producer != null)
            return producer;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, InternalMessageSerde.Serializer.class);
        return new ProducerImpl(new KafkaProducer<>(props));
    }

    public Scheduler build() {
        Producer producer = getProducer();
        TopicRouterImpl router = new TopicRouterImpl(
                checkNotEmpty(topics),
                handler,
                producer
        );
        RoutingScheduler routingScheduler = new RoutingScheduler(router);
        InternalThreadDispatcher dispatcher = new InternalThreadDispatcher(
                topics,
                new ConsumerFactory(servers),
                routingScheduler
        );
        return new Scheduler() {
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
            public void enqueue(Instant instant, ClientMessage message) {
                routingScheduler.enqueue(instant, message);
            }

            @Override
            public void close() {
                dispatcher.close();
                producer.close();
            }
        };
    }
}
