package pt.pak3nuh.messaging.kafka.scheduler;

import pt.pak3nuh.messaging.kafka.scheduler.data.Bytes;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static pt.pak3nuh.messaging.kafka.scheduler.data.Bytes.Reader;

public final class InternalMessage {

    private static final AtomicLong ID_GEN = new AtomicLong(0);

    private final long id;
    private final Instant shouldRunAt;
    private final ClientMessage message;

    public InternalMessage(long id, Instant shouldRunAt, ClientMessage message) {
        this.id = id;
        this.shouldRunAt = shouldRunAt;
        this.message = message;
    }

    public InternalMessage(Instant shouldRunAt, ClientMessage message) {
        this(ID_GEN.getAndIncrement(), shouldRunAt, message);
    }

    public long getId() {
        return id;
    }

    public ClientMessage getClientMessage() {
        return message;
    }

    public Instant shouldRunAt() {
        return shouldRunAt;
    }

    public byte[] toBytes() {
        return new Bytes.Writer(6)
                .putLong(id)
                .putInstant(shouldRunAt)
                .putInstant(message.getCreatedAt())
                .putString(message.getSource())
                .putString(message.getDestination())
                .putBytes(message.getContent())
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
