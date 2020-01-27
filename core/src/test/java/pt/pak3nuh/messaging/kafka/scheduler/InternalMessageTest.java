package pt.pak3nuh.messaging.kafka.scheduler;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class InternalMessageTest {

    @Test
    void shouldSerializeAndDeserialize() {
        InternalMessage original = new InternalMessage(50, Instant.now(), Instant.now(),
                new ClientMessage(Instant.now(), "source".getBytes(), "destination", "content".getBytes()));

        InternalMessage other = InternalMessage.fromBytes(original.toBytes());

        assertNotSame(original, other);
        assertEquals(original, other);
    }
}