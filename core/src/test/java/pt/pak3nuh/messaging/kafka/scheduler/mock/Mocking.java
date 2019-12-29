package pt.pak3nuh.messaging.kafka.scheduler.mock;

import org.mockito.BDDMockito;

public final class Mocking {
    private Mocking() {
    }

    /**
     * Same as {@link BDDMockito#mock(Class)} but throws on unstubbed methods.
     */
    public static <T> T mockStrict(Class<T> clazz) {
        return BDDMockito.mock(clazz, invocation -> {
            throw new UnsupportedOperationException("Unstubbed");
        });
    }
}
