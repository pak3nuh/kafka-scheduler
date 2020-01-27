package pt.pak3nuh.messaging.kafka.scheduler;

public class SchedulerException extends RuntimeException {

    private final boolean isFatal;

    public SchedulerException(boolean isFatal) {
        super();
        this.isFatal = isFatal;
    }

    public SchedulerException(Exception cause) {
        super(cause);
        this.isFatal = false;
    }

    public SchedulerException(String message, Exception cause) {
        super(message, cause);
        this.isFatal = false;
    }

    public boolean isFatal() {
        return isFatal;
    }
}
