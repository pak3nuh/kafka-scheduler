package pt.pak3nuh.messaging.kafka.scheduler.data;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class Bytes {
    private Bytes() {
    }

    public static Charset CHARSET = Charset.defaultCharset();

    public static final class Writer {

        private final List<Value> values;

        /**
         * Creates an unbounded writer that converts values into a byte representation.
         * @param placeholders Number of expected placeholders, not a hard limit.
         */
        public Writer(int placeholders) {
            values = new ArrayList<>(placeholders);
        }

        public Writer putInt(int value) {
            values.add(new Value() {
                @Override
                public int size() {
                    return Integer.BYTES;
                }

                @Override
                public void write(ByteBuffer buffer) {
                    buffer.putInt(value);
                }
            });
            return this;
        }

        public Writer putLong(long value) {
            values.add(new Value() {
                @Override
                public int size() {
                    return Long.BYTES;
                }

                @Override
                public void write(ByteBuffer buffer) {
                    buffer.putLong(value);
                }
            });
            return this;
        }

        public Writer putString(String value) {
            byte[] bytes = value.getBytes(CHARSET);
            return bytesValue(bytes);
        }

        public Writer putBytes(byte[] value) {
            return bytesValue(value);
        }

        private Writer bytesValue(byte[] value) {
            values.add(new Value() {
                @Override
                public int size() {
                    return Integer.BYTES + value.length;
                }

                @Override
                public void write(ByteBuffer buffer) {
                    buffer.putInt(value.length);
                    buffer.put(value);
                }
            });
            return this;
        }

        public Writer putInstant(Instant value) {
            long epochSecond = value.getEpochSecond();
            int epochNano = value.getNano();
            values.add(new Value() {
                @Override
                public int size() {
                    return Long.BYTES + Integer.BYTES;
                }

                @Override
                public void write(ByteBuffer buffer) {
                    buffer.putLong(epochSecond);
                    buffer.putInt(epochNano);
                }
            });
            return this;
        }

        public byte[] toBytes() {
            int bufferSize = 0;
            for (Value value : values) {
                bufferSize += value.size();
            }
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            values.forEach(v -> v.write(buffer));
            return buffer.array();
        }

        private interface Value {
            int size();
            void write(ByteBuffer buffer);
        }
    }

    public static final class Reader {

        private final ByteBuffer buffer;

        public Reader(byte[] bytes) {
            buffer = ByteBuffer.wrap(bytes);
        }

        public int getInt() {
            return buffer.getInt();
        }

        public long getLong() {
            return buffer.getLong();
        }

        public Instant getInstant() {
            long epochSecond = buffer.getLong();
            int epochNano = buffer.getInt();
            return Instant.ofEpochSecond(epochSecond, epochNano);
        }

        public String getString() {
            byte[] data = getBytes();
            return new String(data, CHARSET);
        }

        public byte[] getBytes() {
            int length = buffer.getInt();
            byte[] data = new byte[length];
            buffer.get(data);
            return data;
        }
    }
}


