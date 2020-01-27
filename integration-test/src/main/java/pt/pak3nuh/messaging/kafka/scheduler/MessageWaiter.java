package pt.pak3nuh.messaging.kafka.scheduler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public final class MessageWaiter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageWaiter.class);
    private final String destinationTopic;
    private final String servers;
    private final Duration arrivalDelta;
    private final Scheduler scheduler;
    private final CompletableFuture<Result> future = new CompletableFuture<>();
    private final Map<String, Data> enqueued;
    private volatile boolean closed = false;

    public MessageWaiter(String destinationTopic, String servers, Duration arrivalDelta, Scheduler scheduler,
                         int recordSize) {
        this.destinationTopic = destinationTopic;
        this.servers = servers;
        this.arrivalDelta = arrivalDelta;
        this.scheduler = scheduler;
        enqueued = new ConcurrentHashMap<>(recordSize);
    }

    public void enqueueMessage(String data, Instant arrival) {
        LOGGER.trace("Enqueuing data {} at {}", data, arrival);
        ClientMessage clientMessage = new ClientMessage("message-waiter", destinationTopic, data.getBytes());
        scheduler.enqueue(arrival, clientMessage);
        enqueued.put(data, new Data(data, arrival));
    }

    public Result waitForTermination(Instant timeout) throws ExecutionException, InterruptedException {
        KafkaThread thread = new KafkaThread(timeout);
        thread.start();
        return future.get();
    }

    @Override
    public void close() {
        closed = true;
    }

    private class KafkaThread extends Thread {

        private final KafkaConsumer<byte[], byte[]> consumer;
        private final Instant timeout;
        private final Result result = new Result(enqueued);
        private final int numMessages = enqueued.size();

        private KafkaThread(Instant timeout) {
            setDaemon(true);
            this.timeout = timeout;
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "some test group");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Collections.singleton(destinationTopic));
                pollLoop();
            } catch (Throwable ex) {
                result.throwable = ex;
            } finally {
                future.complete(result);
                consumer.close();
            }
        }

        private void pollLoop() throws InterruptedException, TimeoutException {
            while (!closed) {
                if (result.success == numMessages) {
                    future.complete(result);
                    return;
                }
                checkInterrupted();
                checkTimeout();
                consumer.poll(Duration.ofSeconds(3)).forEach(record -> {
                    Instant now = Instant.now();
                    result.messagesReceived++;
                    String key = new String(record.value());
                    LOGGER.debug("Received key {}", key);
                    Data data = enqueued.get(key);
                    if (null == data) {
                        LOGGER.error("Non expected data {}", key);
                        result.unexpectedData++;
                        return;
                    }
                    data.markArrival();
                    if (data.arrivedAtDelta.size() == 1) {
                        if (now.isAfter(data.expectedAt.plus(arrivalDelta))) {
                            LOGGER.error("Message with id {} expected at {} arrived at {}", data.id, data.expectedAt, now);
                            result.delayed++;
                        } else {
                            result.success++;
                        }
                    } else {
                        LOGGER.error("Key {} received in duplicate", key);
                        result.duplicated++;
                    }
                });
            }
        }

        private void checkTimeout() throws TimeoutException {
            if (Instant.now().isAfter(timeout)) {
                throw new TimeoutException("Wait timed out, remaining " + enqueued);
            }
        }

        private void checkInterrupted() throws InterruptedException {
            if (isInterrupted())
                throw new InterruptedException("Thread interrupted");
        }
    }

    private static class Data {
        private final String id;
        private final Instant expectedAt;
        private final List<Long> arrivedAtDelta = new ArrayList<>();

        private Data(String id, Instant expectedAt) {
            this.id = id;
            this.expectedAt = expectedAt;
        }

        public void markArrival() {
            final long arrivalSeconds = expectedAt.until(Instant.now(), ChronoUnit.SECONDS);
            arrivedAtDelta.add(arrivalSeconds);
        }

        @Override
        public String toString() {
            return "Data{" +
                    "id='" + id + '\'' +
                    ", expectedAt=" + expectedAt +
                    ", arrivedAtDelta=" + arrivedAtDelta +
                    '}';
        }
    }

    public static class Result {
        private int success;
        private int duplicated;
        private int delayed;
        private int unexpectedData;
        private int messagesReceived;
        public final Map<String, Data> originalData;
        public Throwable throwable;

        public Result(Map<String, Data> originalData) {
            this.originalData = originalData;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "success=" + success +
                    ", duplicated=" + duplicated +
                    ", delayed=" + delayed +
                    ", unexpectedData=" + unexpectedData +
                    ", lateValuesMean=" + lateValuesMean() +
                    '}';
        }

        private double lateValuesMean() {
            return originalData.values()
                    .stream().flatMap(it -> it.arrivedAtDelta.stream())
                    .filter(it -> it > 0)
                    .mapToLong(it -> it)
                    .summaryStatistics()
                    .getAverage();
        }

        public boolean isSuccess() {
            return success == originalData.size()
                    && messagesReceived == success
                    && duplicated == 0
                    && delayed == 0
                    && unexpectedData == 0
                    && throwable == null;
        }
    }
}
