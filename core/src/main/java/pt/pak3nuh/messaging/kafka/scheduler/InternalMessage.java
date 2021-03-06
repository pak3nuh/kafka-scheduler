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
    /**
     * The delivery time to the final destination.
     */
    private final Instant deliverAt;
    /**
     * The time at which this message was created. Used for intermediary topics.
     */
    private final Instant createdAt;
    private final ClientMessage clientMessage;

    @VisibleForTesting
    InternalMessage(long id, Instant deliverAt, Instant createdAt, ClientMessage message) {
        this.id = id;
        this.deliverAt = deliverAt;
        this.createdAt = createdAt;
        this.clientMessage = message;
    }

    public InternalMessage(Instant deliverAt, ClientMessage message) {
        this(ID_GEN.getAndIncrement(), deliverAt, Instant.now(), message);
    }

    /**
     * Creates a new copy of this message with a new creation timestamp.
     */
    public InternalMessage copy() {
        return new InternalMessage(id, deliverAt, Instant.now(), clientMessage);
    }

    public byte[] toBytes() {
        return new Bytes.Writer(7)
                .putLong(id)
                .putInstant(deliverAt)
                .putInstant(createdAt)
                .putInstant(clientMessage.getCreatedAt())
                .putBytes(clientMessage.getKey())
                .putString(clientMessage.getDestination())
                .putBytes(clientMessage.getContent())
                .toBytes();
    }

    public static InternalMessage fromBytes(byte[] bytes) {
        Reader reader = new Reader(bytes);
        long id = reader.getLong();
        Instant deliverAt = reader.getInstant();
        Instant internalCreatedAt = reader.getInstant();
        Instant clientCreatedAt = reader.getInstant();
        byte[] key = reader.getBytes();
        String destination = reader.getString();
        byte[] content = reader.getBytes();
        return new InternalMessage(id, deliverAt, internalCreatedAt,
                new ClientMessage(clientCreatedAt, key, destination, content));
    }
}
