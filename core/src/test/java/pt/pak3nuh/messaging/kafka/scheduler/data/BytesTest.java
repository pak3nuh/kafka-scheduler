package pt.pak3nuh.messaging.kafka.scheduler.data;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BytesTest {

    @Test
    void shouldSerializeAndDeserialize() {
        int val1 = -1234567;
        long val2 = 1234567890987654L;
        String val3 = UUID.randomUUID().toString();
        byte[] val4 = UUID.randomUUID().toString().getBytes();
        Instant val5 = Instant.now();

        byte[] bytes = new Bytes.Writer(5)
                .putInt(val1)
                .putLong(val2)
                .putString(val3)
                .putBytes(val4)
                .putInstant(val5)
                .toBytes();

        Bytes.Reader reader = new Bytes.Reader(bytes);
        assertEquals(val1, reader.getInt());
        assertEquals(val2, reader.getLong());
        assertEquals(val3, reader.getString());
        assertArrayEquals(val4, reader.getBytes());
        assertEquals(val5, reader.getInstant());
    }
}