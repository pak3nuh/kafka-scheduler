package pt.pak3nuh.messaging.kafka.scheduler.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;
import pt.pak3nuh.messaging.kafka.scheduler.data.InternalMessageSerde;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Function;

public class ConsumerFactory implements Function<SchedulerTopic, Consumer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerFactory.class);
    public static final StringDeserializer KEY_DESERIALIZER = new StringDeserializer();
    public static final InternalMessageSerde.Deserializer VALUE_DESERIALIZER = new InternalMessageSerde.Deserializer();
    private final Properties consumerConfig;
    private final String appName;

    public ConsumerFactory(String servers, String appName) {
        this.appName = appName;
        this.consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appName);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        LOGGER.info("Building consumer properties for app [{}] on servers [{}]", appName, servers);
    }

    @Override
    public Consumer apply(SchedulerTopic topic) {
        String topicName = topic.topicName();
        LOGGER.info("Creating consumer for topic {} on group {}", topicName, appName);
        KafkaConsumer<String, InternalMessage> consumer = new KafkaConsumer<>(consumerConfig, KEY_DESERIALIZER, VALUE_DESERIALIZER);
        LOGGER.trace("Subscribing consumer on topic {}", topicName);
        consumer.subscribe(Collections.singleton(topicName));
        // todo when the subscription granularity changes this should be revised
        return new ConsumerImpl(consumer, Duration.ofSeconds(10));
    }
}
