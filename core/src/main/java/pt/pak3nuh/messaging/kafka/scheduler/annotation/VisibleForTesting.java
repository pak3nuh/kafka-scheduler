package pt.pak3nuh.messaging.kafka.scheduler.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Denotes an element that is more visible then it should be for testing purposes.
 */
@Retention(RetentionPolicy.SOURCE)
public @interface VisibleForTesting {
}
