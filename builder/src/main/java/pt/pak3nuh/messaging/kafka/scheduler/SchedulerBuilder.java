package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.consumer.ConsumerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.dispatcher.InternalThreadDispatcher;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;
import pt.pak3nuh.messaging.kafka.scheduler.producer.ProducerImpl;
import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouterImpl;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static pt.pak3nuh.messaging.kafka.scheduler.util.Check.*;

public final class SchedulerBuilder {

    private final Set<SchedulerTopic> topics = new HashSet<>();
    private MessageFailureHandler handler = new LoggingFailureHandler();
    private Producer producer = new ProducerImpl();
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

    public Scheduler build() {
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
                dispatcher.start();
            }

            @Override
            public void enqueue(Instant instant, ClientMessage message) {
                routingScheduler.enqueue(instant, message);
            }

            @Override
            public void close() {
                dispatcher.close();
            }
        };
    }
}
