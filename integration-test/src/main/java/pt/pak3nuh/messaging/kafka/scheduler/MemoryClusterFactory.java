package pt.pak3nuh.messaging.kafka.scheduler;

import org.mandas.kafka.KafkaCluster;

import java.util.HashMap;

public final class MemoryClusterFactory {
    private MemoryClusterFactory() {
    }

    public static KafkaCluster create() {
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("auto.create.topics.enable", true);
        return KafkaCluster.builder()
                .withZookeeper("127.0.0.1", 2181, 2182)
                .withBroker(1, "127.0.0.1", 9092, 9093, properties)
                .build();
    }
}
