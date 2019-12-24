package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

/**
 * Asynchronous dispatching mechanism that handles consumption of the internal topics
 */
public interface InternalDispatcher {
    void start();

    void close();
}
