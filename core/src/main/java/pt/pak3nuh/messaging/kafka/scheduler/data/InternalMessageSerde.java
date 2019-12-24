package pt.pak3nuh.messaging.kafka.scheduler.data;

import org.apache.kafka.common.serialization.Serde;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

public class InternalMessageSerde implements Serde<InternalMessage> {

    @Override
    public Serializer serializer() {
        return new Serializer();
    }

    @Override
    public Deserializer deserializer() {
        return new Deserializer();
    }

    public static class Serializer implements org.apache.kafka.common.serialization.Serializer<InternalMessage> {

        @Override
        public byte[] serialize(String topic, InternalMessage internalMessage) {
            return internalMessage.toBytes();
        }
    }

    public static class Deserializer implements org.apache.kafka.common.serialization.Deserializer<InternalMessage> {

        @Override
        public InternalMessage deserialize(String topic, byte[] bytes) {
            return InternalMessage.fromBytes(bytes);
        }
    }
}
