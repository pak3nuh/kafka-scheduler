package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

public final class DispatcherThread extends Thread {

    private DispatcherTopicConfig config;

    public DispatcherThread(DispatcherTopicConfig config) {
        this.config = config;
    }

    {
        setDaemon(true);
    }
}
