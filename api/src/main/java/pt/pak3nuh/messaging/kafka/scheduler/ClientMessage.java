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

    public Instant getCreatedAt() {
        return createdAt;
    }

    public String getDestination() {
        return destination;
    }

    public byte[] getContent() {
        return content;
    }

    public String getSource() {
        return source;
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
