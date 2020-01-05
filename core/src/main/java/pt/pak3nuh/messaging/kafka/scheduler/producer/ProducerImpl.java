package pt.pak3nuh.messaging.kafka.scheduler.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pt.pak3nuh.messaging.kafka.scheduler.ClientMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerImpl implements Producer {

    private final org.apache.kafka.clients.producer.Producer<String, byte[]> producer;

    public ProducerImpl(org.apache.kafka.clients.producer.Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void send(String topic, InternalMessage content) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, String.valueOf(content.getId()), content.toBytes()));
        waitFor(future);
    }

    @Override
    public void send(String topic, ClientMessage content) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, content.getId(), content.getContent()));
        waitFor(future);
    }

    private void waitFor(Future<RecordMetadata> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new SchedulerException(e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
