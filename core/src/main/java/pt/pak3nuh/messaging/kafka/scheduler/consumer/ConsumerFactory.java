package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;
import pt.pak3nuh.messaging.kafka.scheduler.data.InternalMessageSerde;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Function;

public class ConsumerFactory implements Function<SchedulerTopic, Consumer> {
    private final Properties consumerConfig;

    public ConsumerFactory(String servers, String appName) {
        this.consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appName);
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, InternalMessageSerde.Deserializer.class);
    }

    @Override
    public Consumer apply(SchedulerTopic topic) {
        KafkaConsumer<String, InternalMessage> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singleton(topic.topicName()));
        return new ConsumerImpl(consumer, Duration.ofMinutes(1));
    }
}
