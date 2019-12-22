package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;
import pt.pak3nuh.messaging.kafka.scheduler.producer.ProducerImpl;
import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouterImpl;

import java.util.HashSet;
import java.util.Set;

public final class SchedulerBuilder {

    private final Set<SchedulerTopic> topics = new HashSet<>();
    private MessageFailureHandler handler = new LoggingFailureHandler();
    private Producer producer = new ProducerImpl();

    public SchedulerBuilder addScheduleMinutes(int minutes) {
        topics.add(new SchedulerTopic(minutes, SchedulerTopic.Granularity.MINUTES));
        return this;
    }

    public SchedulerBuilder addScheduleHours(int hours) {
        topics.add(new SchedulerTopic(hours, SchedulerTopic.Granularity.HOURS));
        return this;
    }

    public SchedulerBuilder failureHandler(MessageFailureHandler handler) {
        this.handler = handler;
        return this;
    }

    public SchedulerBuilder producer(Producer producer) {
        this.producer = producer;
        return this;
    }

    public Scheduler build() {
        return new SchedulerImpl(new TopicRouterImpl(topics, handler, producer));
    }
}
