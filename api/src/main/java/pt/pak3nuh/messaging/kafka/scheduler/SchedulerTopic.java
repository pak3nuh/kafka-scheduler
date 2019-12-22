package pt.pak3nuh.messaging.kafka.scheduler;

import java.util.Objects;

public final class SchedulerTopic {
    private final int holdValue;
    private final Granularity granularity;

    public SchedulerTopic(int holdValue, Granularity granularity) {
        this.holdValue = holdValue;
        this.granularity = granularity;
    }

    public int getHoldValue() {
        return holdValue;
    }

    public Granularity getGranularity() {
        return granularity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchedulerTopic that = (SchedulerTopic) o;
        return holdValue == that.holdValue &&
                granularity == that.granularity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(holdValue, granularity);
    }

    public enum Granularity {
        MINUTES, HOURS
    }
}
