package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class InternalThreadDispatcher implements InternalDispatcher {

    private final Set<DispatcherTopicConfig> topicConfigSet;
    private final List<DispatcherThread> threads;

    public InternalThreadDispatcher(Set<DispatcherTopicConfig> topicConfigSet) {
        this.topicConfigSet = topicConfigSet;
        threads = new ArrayList<>(topicConfigSet.size());
    }

    @Override
    public void start() {
        topicConfigSet.stream()
                .map(this::createDispatcherThread)
                .forEach(threads::add);
    }

    private DispatcherThread createDispatcherThread(DispatcherTopicConfig dispatcherTopicConfig) {
        return null;
    }

    @Override
    public void close() {

    }
}
