package pt.pak3nuh.messaging.kafka.scheduler.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pt.pak3nuh.messaging.kafka.scheduler.ClientMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerImpl implements Producer {

    private final org.apache.kafka.clients.producer.Producer<byte[], byte[]> producer;
    private volatile boolean closed = false;

    public ProducerImpl(org.apache.kafka.clients.producer.Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void send(String topic, InternalMessage internal) {
        checkClosed();
        byte[] idBytes = ByteBuffer.allocate(Long.BYTES).putLong(internal.getId()).array();
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, idBytes, internal.toBytes()));
        waitFor(future);
    }

    @Override
    public void send(String topic, ClientMessage client) {
        checkClosed();
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, client.getKey(), client.getContent()));
        waitFor(future);
    }

    private void waitFor(Future<RecordMetadata> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SchedulerException(e);
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new ProducerClosedException();
        }
    }

    @Override
    public void close() {
        closed = true;
        producer.close();
    }
}
