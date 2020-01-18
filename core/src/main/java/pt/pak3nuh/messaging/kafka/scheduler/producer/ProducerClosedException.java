package pt.pak3nuh.messaging.kafka.scheduler.producer;

import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;

public final class ProducerClosedException extends SchedulerException {
    public ProducerClosedException() {
        super(true);
    }
}
