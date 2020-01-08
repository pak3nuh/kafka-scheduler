package pt.pak3nuh.messaging.kafka.scheduler.data;

import lombok.Value;

public class Tuples {
    private Tuples() {
    }

    /**
     * Value based tuple with the equals and hashcode based on the values.
     * @param <L>
     * @param <R>
     */
    @Value
    public static class Tuple<L, R> {
        L left;
        R right;
    }
}
