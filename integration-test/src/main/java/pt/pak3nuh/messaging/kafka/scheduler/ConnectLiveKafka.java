package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;

public final class ConnectLiveKafka {
    public static void main(String[] args) {
        SchedulerBuilder builder = new SchedulerBuilder("localhost:9092").addScheduleMinutes(1)
                .addScheduleMinutes(5)
                .addScheduleMinutes(20)
                .appName("blah");

        try(Scheduler scheduler = builder.build()) {
            ClientMessage clientMessage = new ClientMessage("blah-app".getBytes(), "destination-topic", new byte[0]);
            scheduler.enqueue(Instant.now().plusSeconds(120), clientMessage);
        }
    }
}
