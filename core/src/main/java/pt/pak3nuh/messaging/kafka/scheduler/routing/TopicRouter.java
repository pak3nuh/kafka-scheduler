package pt.pak3nuh.messaging.kafka.scheduler.routing;

import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.Topic;

public interface TopicRouter {
    Topic nextTopic(InternalMessage message);
}
