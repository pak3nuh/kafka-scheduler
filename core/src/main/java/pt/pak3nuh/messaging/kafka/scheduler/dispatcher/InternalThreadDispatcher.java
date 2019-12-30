package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessageHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;
import pt.pak3nuh.messaging.kafka.scheduler.annotation.VisibleForTesting;
import pt.pak3nuh.messaging.kafka.scheduler.consumer.Consumer;
import pt.pak3nuh.messaging.kafka.scheduler.data.Tuples;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public final class InternalThreadDispatcher implements InternalDispatcher {

    private final Set<SchedulerTopic> topicConfigSet;
    private final List<DispatcherThread> threads;
    private final Supplier<Consumer> consumerFactory;
    private final InternalMessageHandler internalMessageHandler;

    public InternalThreadDispatcher(Set<SchedulerTopic> topicConfigSet, Supplier<Consumer> consumerFactory,
                                    InternalMessageHandler internalMessageHandler) {
        this.topicConfigSet = topicConfigSet;
        threads = new ArrayList<>(topicConfigSet.size());
        this.consumerFactory = consumerFactory;
        this.internalMessageHandler = internalMessageHandler;
    }

    @Override
    public void start() {
        topicConfigSet.stream()
                .map(this::createDispatcherThread)
                .forEach(threads::add);
    }

    private DispatcherThread createDispatcherThread(SchedulerTopic dispatcherTopicConfig) {
        Consumer consumer = consumerFactory.get();
        DispatcherThread thread = new DispatcherThread(dispatcherTopicConfig, consumer, internalMessageHandler);
        thread.start();
        return thread;
    }

    @Override
    public void close() {
        Iterator<DispatcherThread> iterator = threads.iterator();
        while (iterator.hasNext()) {
            iterator.next().shutdown();
            iterator.remove();
        }
    }

    static final class DispatcherThread extends Thread {

        private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherThread.class);
        private final SchedulerTopic config;
        private final Consumer consumer;
        private final InternalMessageHandler internalMessageHandler;
        private volatile boolean closing;

        public DispatcherThread(SchedulerTopic config, Consumer consumer, InternalMessageHandler internalMessageHandler) {
            this.config = config;
            this.consumer = consumer;
            this.internalMessageHandler = internalMessageHandler;
        }

        {
            setDaemon(true);
        }

        public void shutdown() {
            closing = true;
        }

        @Override
        public void run() {
            while (!closing) {
                try {
                    doConsume();
                } catch (WakeupException ex) {
                    LOGGER.warn("Wakeup exception occurred, closing loop", ex);
                    closing = true;
                } catch (Exception ex) {
                    LOGGER.error("Error doing consumer loop", ex);
                }
            }
            consumer.close();
        }

        @VisibleForTesting
        void doConsume() {
            Iterable<Consumer.Record> records = consumer.poll();
            Work work = splitWork(records, Instant.now());
            work.toProcess.forEach(record -> {
                internalMessageHandler.handle(record.getMessage());
                consumer.commit(record);
            });
            work.toPause.forEach(tuple -> consumer.pause(tuple.getLeft(), tuple.getRight()));
        }

        @VisibleForTesting
        Work splitWork(Iterable<Consumer.Record> records, Instant now) {
            Work work = new Work();
            // avoid calculating the enqueued with delay for each record
            Instant nowWithDelay = now.minus(config.toSeconds(), ChronoUnit.SECONDS);
            records.forEach(record -> {
                long secondsToProcess = secondsUntilProcess(record, nowWithDelay);
                if (secondsToProcess <= 0) {
                    work.toProcess.add(record);
                } else {
                    Instant until = now.plusSeconds(secondsToProcess);
                    work.toPause.add(new Tuples.Tuple<>(record, until));
                }
            });
            return work;
        }

        private long secondsUntilProcess(Consumer.Record record, Instant nowWithDelay) {
            return nowWithDelay.until(record.getMessage().getCreatedAt(), ChronoUnit.SECONDS);
        }

        @VisibleForTesting
        static class Work {
            List<Consumer.Record> toProcess = new ArrayList<>();
            List<Tuples.Tuple<Consumer.Record, Instant>> toPause = new ArrayList<>();
        }

    }
}
