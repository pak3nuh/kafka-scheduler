package pt.pak3nuh.messaging.kafka.scheduler.routing;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.MessageFailureHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;
import pt.pak3nuh.messaging.kafka.scheduler.Topic;

public class FailureTopic implements Topic {

    private final MessageFailureHandler handler;
    private final SchedulerException cause;

    public FailureTopic(MessageFailureHandler handler, SchedulerException cause) {
        this.handler = handler;
        this.cause = cause;
    }

    @Override
    public void send(InternalMessage content) {
        handler.handle(content.getClientMessage(), cause);
    }
}
