package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.stream.Stream;

public final class InternalMessageFactory {

    public static InternalMessage create() {
        return create(0, 0);
    }

    public static InternalMessage create(int minutes, int hours) {
        Instant timestamp = Instant.now().plus(minutes, ChronoUnit.MINUTES).plus(hours, ChronoUnit.HOURS);
        return create(timestamp);
    }

    public static InternalMessage create(Instant timestamp) {
        return create(timestamp, timestamp);
    }

    public static InternalMessage create(Instant deliveryTime, Instant creationTime) {
        return new InternalMessage(0, deliveryTime, creationTime,
                new ClientMessage(Instant.now(), "source", "destination", new byte[0]));
    }

    public static Stream<InternalMessage> createInternalMessageStream(int numberOfMessages, int minutes) {
        ArrayList<InternalMessage> objects = new ArrayList<>(numberOfMessages);
        Instant delivery = Instant.now().plusSeconds(60 * minutes);
        for (int i = 0; i < numberOfMessages; i++) {
            objects.add(create(delivery, Instant.now()));
        }
        return objects.stream();
    }
}
