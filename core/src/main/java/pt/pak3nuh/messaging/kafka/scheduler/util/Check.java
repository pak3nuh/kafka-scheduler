package pt.pak3nuh.messaging.kafka.scheduler.util;

import java.util.Collection;
import java.util.Objects;

public class Check {
    private Check() {
    }

    public static <T, C extends Collection<T>> C checkNotEmpty(C collection) {
        check(!collection.isEmpty(), "Collection is empty");
        return collection;
    }

    public static void checkPositive(long number) {
        check(number > 0, "Number is not bigger than zero");
    }

    public static <T> T checkNotNull(T object) {
        return Objects.requireNonNull(object);
    }

    public static String checkNotEmpty(String string) {
        check(string != null && !string.isEmpty(), "String is null or empty");
        return string;
    }

    public static void check(boolean check, String message) {
        if(!check) {
            throw new IllegalArgumentException(message);
        }
    }
}
