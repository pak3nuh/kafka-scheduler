package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

import lombok.ToString;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessageHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;
import pt.pak3nuh.messaging.kafka.scheduler.annotation.VisibleForTesting;
import pt.pak3nuh.messaging.kafka.scheduler.consumer.Consumer;
import pt.pak3nuh.messaging.kafka.scheduler.data.Tuples;
import pt.pak3nuh.messaging.kafka.scheduler.util.Check;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class InternalThreadDispatcher implements InternalDispatcher, Thread.UncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(InternalThreadDispatcher.class);
    private final Set<SchedulerTopic> topicConfigSet;
    private final List<DispatcherThread> threads;
    private final Function<SchedulerTopic, Consumer> consumerFactory;
    private final InternalMessageHandler internalMessageHandler;

    public InternalThreadDispatcher(Set<SchedulerTopic> topicConfigSet,
                                    Function<SchedulerTopic, Consumer> consumerFactory,
                                    InternalMessageHandler internalMessageHandler) {
        this.topicConfigSet = topicConfigSet;
        threads = new ArrayList<>(topicConfigSet.size());
        this.consumerFactory = consumerFactory;
        this.internalMessageHandler = internalMessageHandler;
    }

    @Override
    public void start() {
        LOGGER.info("Starting dispatcher threads");
        topicConfigSet.stream()
                .map(this::createDispatcherThread)
                .forEach(threads::add);
    }

    private DispatcherThread createDispatcherThread(SchedulerTopic dispatcherTopicConfig) {
        Consumer consumer = consumerFactory.apply(dispatcherTopicConfig);
        DispatcherThread thread = new DispatcherThread(dispatcherTopicConfig, consumer, internalMessageHandler);
        thread.setUncaughtExceptionHandler(this);
        thread.start();
        return thread;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        Check.check(thread instanceof DispatcherThread, "Unknown thread");
        DispatcherThread dispatcherThread = (DispatcherThread) thread;
        LOGGER.error("DispatcherThread {} died with uncaught exception. Topic {} is unhandled",
                thread.getName(), dispatcherThread.config);
        // Doesn't launch another thread so that it doesn't loop on cases like out of memory or a persistent error
        // todo set a handler to configure what to do
    }

    @Override
    public void close() {
        LOGGER.info("Shutting down dispatcher threads");
        Iterator<DispatcherThread> iterator = threads.iterator();
        while (iterator.hasNext()) {
            iterator.next().shutdown();
            iterator.remove();
        }
    }

    static final class DispatcherThread extends Thread {

        private static AtomicInteger THREAD_ID_GEN = new AtomicInteger(0);
        private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherThread.class);
        private final SchedulerTopic config;
        private final Consumer consumer;
        private final InternalMessageHandler internalMessageHandler;
        private volatile boolean closing;

        public DispatcherThread(SchedulerTopic config, Consumer consumer, InternalMessageHandler internalMessageHandler) {
            this.config = config;
            this.consumer = consumer;
            this.internalMessageHandler = internalMessageHandler;
            setName("DispatcherThread-" + THREAD_ID_GEN.getAndIncrement());
        }

        {
            setDaemon(true);
        }

        public void shutdown() {
            // different thread signals the shutdown
            LOGGER.info("Signaling shutdown on thread {}", getName());
            closing = true;
        }

        @Override
        public void run() {
            LOGGER.info("Starting consumer loop");
            try {
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
            } finally {
                LOGGER.info("Ending consumer loop");
                consumer.close();
            }
        }

        @VisibleForTesting
        void doConsume() {
            Iterable<Consumer.Record> records = consumer.poll();
            Work work = splitWork(records, Instant.now());
            LOGGER.debug("Work to process {}", work);
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
            LOGGER.trace("Now offsetted with topic hold time {}", nowWithDelay);
            records.forEach(record -> {
                long secondsToProcess = secondsUntilProcess(record.getMessage(), nowWithDelay, now);
                LOGGER.trace("Seconds {} to process record record {}", secondsToProcess, record);
                if (secondsToProcess <= 0) {
                    work.toProcess.add(record);
                } else {
                    Instant until = now.plusSeconds(secondsToProcess);
                    work.toPause.add(new Tuples.Tuple<>(record, until));
                }
            });
            return work;
        }

        /**
         * Returns the remaining seconds until the record can be processed.
         *
         * @param message      The message to process.
         * @param nowWithDelay The timestamp offsetted for the topic hold time.
         * @param now          Current timestamp.
         * @return A positive number if there are seconds to wait or else if it can be processed.
         */
        @VisibleForTesting
        static long secondsUntilProcess(InternalMessage message, Instant nowWithDelay, Instant now) {
            Instant messageDelivery = message.getDeliverAt();
            Instant createdAt = message.getCreatedAt();
            // should hold
            return Math.min(
                    // time until hold time is complete
                    nowWithDelay.until(createdAt, ChronoUnit.SECONDS),
                    // time until message is delivered
                    now.until(messageDelivery, ChronoUnit.SECONDS)
            );
        }

        @VisibleForTesting
        @ToString
        static class Work {
            List<Consumer.Record> toProcess = new ArrayList<>();
            List<Tuples.Tuple<Consumer.Record, Instant>> toPause = new ArrayList<>();
        }

    }
}
