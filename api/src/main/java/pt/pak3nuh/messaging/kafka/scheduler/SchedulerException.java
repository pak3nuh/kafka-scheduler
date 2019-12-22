package pt.pak3nuh.messaging.kafka.scheduler;

public class SchedulerException extends RuntimeException {

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(Exception cause) {
        super(cause);
    }
}
