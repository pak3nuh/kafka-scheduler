package pt.pak3nuh.messaging.kafka.scheduler.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.*;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public final class TopicRouterImpl implements TopicRouter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicRouterImpl.class);
    // sorted from the higher hold value to the lower
    private final SortedSet<HoldTopic> topics;
    private final MessageFailureHandler handler;
    private final Producer producer;

    public TopicRouterImpl(Set<SchedulerTopic> topics, MessageFailureHandler handler, Producer producer) {
        this.topics = topics.stream()
                .map(HoldTopic::new)
                .collect(() -> new TreeSet<>(Comparator.reverseOrder()), TreeSet::add, TreeSet::addAll);
        this.handler = handler;
        this.producer = producer;
    }

    @Override
    public Topic nextTopic(InternalMessage message) {
        Instant now = Instant.now();
        Instant messageRunTime = message.getShouldRunAt();
        if (messageRunTime.compareTo(now) >= 0) {
            return new SinkTopic(producer, message.getClientMessage().getDestination(), handler);
        }

        long secondsToHold = now.until(messageRunTime, ChronoUnit.SECONDS);
        return calculateNextTopic(secondsToHold, message);
    }

    private Topic calculateNextTopic(long secondsToHold, InternalMessage message) {
        return topics.stream()
                .filter(holdTopic -> holdTopic.canHold(secondsToHold) )
                .findFirst()
                .map(holdTopic -> (Topic) new SinkTopic(producer, holdTopic.name, handler))
                .orElse(failure(secondsToHold, message));
    }

    private Topic failure(long secondsToHold, InternalMessage message) {
        LOGGER.error("Couldn't find any suitable topic to hold for {} seconds on message {}", secondsToHold, message);
        return new FailureTopic(handler,
                new SchedulerException("Couldn't find any suitable topic to hold for " + secondsToHold + " seconds"));
    }

    private static class HoldTopic implements Comparable<HoldTopic> {
        private final String name;
        private final long secondsToHold;

        private HoldTopic(SchedulerTopic topic) {
            this.name = topic.topicName();
            this.secondsToHold = topic.toSeconds();
        }

        // the topic hold time can't surpass the message hold time
        public boolean canHold(long timeInSeconds) {
            return secondsToHold <= timeInSeconds;
        }

        @Override
        public int compareTo(HoldTopic holdTopic) {
            return (int) (secondsToHold - holdTopic.secondsToHold);
        }

        @Override
        public String toString() {
            return "HoldTopic{" +
                    "name='" + name + '\'' +
                    ", secondsToHold=" + secondsToHold +
                    '}';
        }
    }
}
