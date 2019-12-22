package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Supplier;

public class ConsumerFactory implements Supplier<Consumer> {
    private Properties consumerConfig;

    public ConsumerFactory(String servers) {
        this.consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, InternalMessageSerde.Deserializer.class);
    }

    @Override
    public Consumer get() {
        KafkaConsumer<String, InternalMessage> consumer = new KafkaConsumer<>(consumerConfig);
        return new ConsumerImpl(consumer, Duration.ofMinutes(1));
    }
}
