package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

public class ClientMessage {
    private final Instant createdAt;
    private final String source;
    private final String destination;
    private final byte[] content;

    public ClientMessage(Instant createdAt, String source, String destination, byte[] content) {
        this.createdAt = Objects.requireNonNull(createdAt);
        this.source = Objects.requireNonNull(source);
        this.destination = Objects.requireNonNull(destination);
        this.content = Objects.requireNonNull(content);
    }

    public ClientMessage(String source, String destination, byte[] content) {
        this(Instant.now(), source, destination, content);
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
     * The content to be delivered.
     */
    public byte[] getContent() {
        return content;
    }

    /**
     * <p>The source of the message. This field isn't handled by the library except for logging purposes</p>
     */
    public String getSource() {
        return source;
    }

    /**
     * <p>Expected message id on delivery to the {@link #getDestination()} topic.</p>
     * <p>This id serves for partitioning purposes only and its uniqueness isn't guaranteed.</p>
     * <p>This may change in the future if partitioning becomes customizable.</p>
     */
    public String getId() {
        int result = Objects.hash(source, destination);
        result = 31 * result + Arrays.hashCode(content);
        return String.valueOf(result);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMessage message = (ClientMessage) o;
        return Objects.equals(createdAt, message.createdAt) &&
                Objects.equals(source, message.source) &&
                Objects.equals(destination, message.destination) &&
                Arrays.equals(content, message.content);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(createdAt, source, destination);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "createdAt=" + createdAt +
                ", source='" + source + '\'' +
                ", destination='" + destination + '\'' +
                '}';
    }
}
