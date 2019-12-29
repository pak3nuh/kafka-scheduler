package pt.pak3nuh.messaging.kafka.scheduler.routing;

import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;
import pt.pak3nuh.messaging.kafka.scheduler.Topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory.createInternalMessage;

class TopicRouterImplTest {

    private final SchedulerTopic fiveMin = new SchedulerTopic(5, SchedulerTopic.Granularity.MINUTES);
    private final SchedulerTopic fiveHours = new SchedulerTopic(5, SchedulerTopic.Granularity.HOURS);
    private final SchedulerTopic tenHours = new SchedulerTopic(10, SchedulerTopic.Granularity.HOURS);

    @Test
    void shouldReturnNextValidTopic() {
        TopicRouterImpl router = new TopicRouterImpl(Sets.newSet(fiveMin, fiveHours, tenHours), null, null);

        assertTopic(router.nextTopic(createInternalMessage(10, 0)), fiveMin.toSeconds());
        assertTopic(router.nextTopic(createInternalMessage(10, 5)), fiveHours.toSeconds());
        assertTopic(router.nextTopic(createInternalMessage(10, 10)), tenHours.toSeconds());
    }

    private void assertTopic(Topic topic, long secondsTopic) {
        assertTrue(topic instanceof SinkTopic);
        SinkTopic sinkTopic = (SinkTopic) topic;
        String secondsAsString = String.valueOf(secondsTopic);
        // this is an implementation detail, because the topic name is standardized with the wait in seconds
        String destination = sinkTopic.getDestination();
        assertTrue(destination.endsWith(secondsAsString), "Got: " + destination + ", expected: " + secondsAsString);
    }

    @Test
    void shouldScheduleImmediatelyOnPastMessage() {
        TopicRouterImpl router = new TopicRouterImpl(Sets.newSet(fiveMin), null, null);
        Topic topic = router.nextTopic(createInternalMessage());
        assertTrue(topic instanceof SinkTopic);
        String destination = ((SinkTopic) topic).getDestination();
        assertEquals("destination", destination);
    }

    @Test
    void shouldReturnFinerGranularityTopicForCompleteWaits() {
        TopicRouterImpl router = new TopicRouterImpl(Sets.newSet(fiveMin, fiveHours), null, null);

        Topic topic = router.nextTopic(createInternalMessage(1, 0));
        assertTopic(topic, fiveMin.toSeconds());
    }
}