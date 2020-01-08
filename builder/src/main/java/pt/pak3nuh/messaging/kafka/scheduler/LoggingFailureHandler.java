package pt.pak3nuh.messaging.kafka.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingFailureHandler implements MessageFailureHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFailureHandler.class);

    @Override
    public void handle(ClientMessage message, SchedulerException cause) {
        LOGGER.error("Couldn't process message {}", message, cause);
    }
}
