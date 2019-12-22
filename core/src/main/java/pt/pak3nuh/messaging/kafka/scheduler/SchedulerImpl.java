package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.routing.TopicRouter;

import java.time.Instant;

public class SchedulerImpl implements Scheduler, InternalMessageHandler {

    private TopicRouter orchestrator;

    public SchedulerImpl(TopicRouter orchestrator) {
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
}
