package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouter;

import java.time.Instant;

public class RoutingScheduler implements Scheduler, InternalMessageHandler {

    private final TopicRouter orchestrator;

    public RoutingScheduler(TopicRouter orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public void enqueue(Instant instant, ClientMessage message) {
        InternalMessage internalMessage = new InternalMessage(instant, message);
        handle(internalMessage);
    }

    @Override
    public void handle(InternalMessage internalMessage) {
        Topic topic = orchestrator.nextTopic(internalMessage);
        topic.send(internalMessage);
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
