package pt.pak3nuh.messaging.kafka.scheduler;

import lombok.Value;
import pt.pak3nuh.messaging.kafka.scheduler.annotation.VisibleForTesting;
import pt.pak3nuh.messaging.kafka.scheduler.data.Bytes;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static pt.pak3nuh.messaging.kafka.scheduler.data.Bytes.Reader;

@Value
public final class InternalMessage {

    private static final AtomicLong ID_GEN = new AtomicLong(0);

    private final long id;
    private final Instant shouldRunAt;
    private final ClientMessage clientMessage;

    @VisibleForTesting
    InternalMessage(long id, Instant shouldRunAt, ClientMessage message) {
        this.id = id;
        this.shouldRunAt = shouldRunAt;
        this.clientMessage = message;
    }

    public InternalMessage(Instant shouldRunAt, ClientMessage message) {
        this(ID_GEN.getAndIncrement(), shouldRunAt, message);
    }

    public byte[] toBytes() {
        return new Bytes.Writer(6)
                .putLong(id)
                .putInstant(shouldRunAt)
                .putInstant(clientMessage.getCreatedAt())
                .putString(clientMessage.getSource())
                .putString(clientMessage.getDestination())
                .putBytes(clientMessage.getContent())
                .toBytes();
    }

    public static InternalMessage fromBytes(byte[] bytes) {
        Reader reader = new Reader(bytes);
        long id = reader.getLong();
        Instant shouldRunAt = reader.getInstant();
        Instant createdAt = reader.getInstant();
        String source = reader.getString();
        String destination = reader.getString();
        byte[] content = reader.getBytes();
        return new InternalMessage(id, shouldRunAt,
                new ClientMessage(createdAt, source, destination, content));
    }
}
