package pt.pak3nuh.messaging.kafka.scheduler;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class InternalMessageFactory {

    public static InternalMessage createInternalMessage() {
        return createInternalMessage(0, 0);
    }

    public static InternalMessage createInternalMessage(int minutes, int hours) {
        Instant toRun = Instant.now().plus(minutes, ChronoUnit.MINUTES).plus(hours, ChronoUnit.HOURS);
        return new InternalMessage(toRun,
                new ClientMessage(Instant.now(), "source", "destination", new byte[0]));
    }
}
