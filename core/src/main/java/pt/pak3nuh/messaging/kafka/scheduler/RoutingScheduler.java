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
        routeMessage(internalMessage);
    }

    @Override
    public void handle(InternalMessage internalMessage) {
        InternalMessage newMessage = internalMessage.copy();
        routeMessage(newMessage);
    }

    private void routeMessage(InternalMessage newMessage) {
        Topic topic = router.nextTopic(newMessage);
        topic.send(newMessage);
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
