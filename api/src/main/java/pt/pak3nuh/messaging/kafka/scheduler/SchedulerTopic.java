package pt.pak3nuh.messaging.kafka.scheduler;

import java.util.Objects;

public final class SchedulerTopic {
    private static final String TOPIC_PREFIX = "kafka-scheduler-internal-seconds-";
    private final int holdValue;
    private final Granularity granularity;
    private final String appName;

    public SchedulerTopic(int holdValue, Granularity granularity, String appName) {
        this.holdValue = holdValue;
        this.granularity = granularity;
        this.appName = appName;
    }

    public int getHoldValue() {
        return holdValue;
    }

    public Granularity getGranularity() {
        return granularity;
    }

    public long toSeconds() {
        switch (granularity) {
            case MINUTES: return holdValue * 60;
            case HOURS: return holdValue * 60 * 60;
        }
        throw new IllegalArgumentException("Unknown granularity " + granularity);
    }

    public String topicName() {
        return appName + "-" + TOPIC_PREFIX + toSeconds();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchedulerTopic that = (SchedulerTopic) o;
        return holdValue == that.holdValue &&
                granularity == that.granularity &&
                Objects.equals(appName, that.appName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(holdValue, granularity, appName);
    }

    @Override
    public String toString() {
        return "SchedulerTopic{" +
                "holdValue=" + holdValue +
                ", granularity=" + granularity +
                ", appName='" + appName + '\'' +
                '}';
    }

    public enum Granularity {
        MINUTES, HOURS
    }
}
