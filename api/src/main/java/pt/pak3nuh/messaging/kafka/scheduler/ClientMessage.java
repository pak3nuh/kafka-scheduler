package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

public class ClientMessage {
    private final Instant createdAt;
    private final byte[] key;
    private final String destination;
    private final byte[] content;

    public ClientMessage(Instant createdAt, byte[] key, String destination, byte[] content) {
        this.createdAt = Objects.requireNonNull(createdAt);
        this.key = Objects.requireNonNull(key);
        this.destination = Objects.requireNonNull(destination);
        this.content = Objects.requireNonNull(content);
    }

    public ClientMessage(byte[] key, String destination, byte[] content) {
        this(Instant.now(), key, destination, content);
    }

    /**
     * Message creation time.
     */
    public Instant getCreatedAt() {
        return createdAt;
    }

    /**
     * <p>Topic to deliver the message once the expected wait time has passed.</p>
     */
    public String getDestination() {
        return destination;
    }

    /**
     * Expected message content on delivery to the {@link #getDestination()} topic.
     */
    public byte[] getContent() {
        return content;
    }

    /**
     * <p>Expected message key on delivery to the {@link #getDestination()} topic.</p>
     */
    public byte[] getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMessage message = (ClientMessage) o;
        return Objects.equals(createdAt, message.createdAt) &&
                Arrays.equals(key, message.key) &&
                Objects.equals(destination, message.destination) &&
                Arrays.equals(content, message.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(createdAt, key, destination);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "createdAt=" + createdAt +
                ", key='" + Arrays.toString(key) + '\'' +
                ", destination='" + destination + '\'' +
                '}';
    }
}
