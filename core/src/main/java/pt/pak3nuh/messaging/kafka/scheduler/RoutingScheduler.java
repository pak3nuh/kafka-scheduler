package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouter;

import java.time.Instant;

public class RoutingScheduler implements Scheduler, InternalMessageHandler {

    private final TopicRouter router;
    private final long granularity;

    public RoutingScheduler(TopicRouter router, long granularity) {
        this.router = router;
        this.granularity = granularity;
    }

    @Override
    public void enqueue(Instant deliverAt, ClientMessage message) {
        InternalMessage internalMessage = new InternalMessage(deliverAt, message);
        handle(internalMessage);
    }

    @Override
    public void handle(InternalMessage internalMessage) {
        Topic topic = router.nextTopic(internalMessage);
        topic.send(internalMessage);
    }

    @Override
    public long granularityInSeconds() {
        return granularity;
    }

    @Override
    public void start() {
        // noop
    }

    @Override
    public void close() {
        // noop
    }
}
