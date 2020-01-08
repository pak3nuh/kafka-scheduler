package pt.pak3nuh.messaging.kafka.scheduler.dispatcher;

import lombok.Value;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessageHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerTopic;
import pt.pak3nuh.messaging.kafka.scheduler.consumer.Consumer;
import pt.pak3nuh.messaging.kafka.scheduler.dispatcher.InternalThreadDispatcher.DispatcherThread;
import pt.pak3nuh.messaging.kafka.scheduler.mock.Mocking;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;
import static pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory.create;
import static pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory.createInternalMessageStream;

class InternalThreadDispatcherTest {

    private static final String APP_NAME = "app-name";
    private Consumer consumer = Mocking.mockStrict(Consumer.class);
    private InternalMessageHandler handler = Mocking.mockStrict(InternalMessageHandler.class);

    {
        willDoNothing().given(handler).handle(any());
        willDoNothing().given(consumer).commit(any());
        willDoNothing().given(consumer).pause(any(), any());
    }

    @Test
    void shouldConsumerRecords() {
        int delay = 1;
        int numberOfRecords = 5;
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(delay, SchedulerTopic.Granularity.MINUTES, APP_NAME), consumer, handler);
        List<R> recordList = createInternalMessageStream(numberOfRecords, -delay).map(R::new).collect(Collectors.toList());
        willReturn(recordList).given(consumer).poll();

        thread.doConsume();

        // all could be processed
        verify(handler, new Times(numberOfRecords)).handle(any(InternalMessage.class));
        verify(consumer, new Times(0)).pause(any(), any());
    }

    @Test
    void shouldPausePartitions() {
        int delay = 1;
        int numberOfRecords = 5;
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(delay, SchedulerTopic.Granularity.MINUTES, APP_NAME), consumer, handler);
        List<R> recordList = createInternalMessageStream(numberOfRecords, 2).map(R::new).collect(Collectors.toList());
        willReturn(recordList).given(consumer).poll();

        thread.doConsume();

        // all were paused
        verify(handler, new Times(0)).handle(any(InternalMessage.class));
        verify(consumer, new Times(numberOfRecords)).pause(any(), any());
    }

    @Test
    void shouldConsumeSomeAndPauseSome() {
        int delay = 1;
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(delay, SchedulerTopic.Granularity.MINUTES, APP_NAME), consumer, handler);
        Stream<R> toConsume = createInternalMessageStream(2, -delay).map(R::new);
        Stream<R> toPause = createInternalMessageStream(2, 2).map(R::new);
        willReturn(Stream.concat(toConsume, toPause).collect(Collectors.toList())).given(consumer).poll();

        thread.doConsume();

        // all were paused
        verify(handler, new Times(2)).handle(any(InternalMessage.class));
        verify(consumer, new Times(2)).pause(any(), any());
    }

    @Test
    void shouldDelayRespectingTheHoldValue() {
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(5, SchedulerTopic.Granularity.MINUTES, APP_NAME), null, null);
        Instant now = Instant.now();
        InternalMessage message = create(now.plusSeconds(10 * 60), now);

        DispatcherThread.Work work = thread.splitWork(singletonList(new R(message)), now);

        assertTrue(work.toProcess.isEmpty());
        assertEquals(1, work.toPause.size());

        Instant resumeAt = work.toPause.get(0).getRight();

        Instant expectedTimestamp = now.plus(5, ChronoUnit.MINUTES);
        assertEquals(expectedTimestamp, resumeAt);
    }


    @Test
    void shouldAddToProcessAfterTheHoldTime() {
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(5, SchedulerTopic.Granularity.MINUTES, APP_NAME), null, null);
        InternalMessage message = create(0, 0);

        // simulates 1 ms after hold
        Instant processTime = message.getCreatedAt().plus(5, ChronoUnit.MINUTES).plusMillis(1);
        DispatcherThread.Work work = thread.splitWork(singletonList(new R(message)), processTime);

        assertTrue(work.toPause.isEmpty());
        assertEquals(1, work.toProcess.size());
    }

    @Test
    void shouldShortCircuitIfMessageIsToBeDelivered() {
        Instant messageDeliveryTime = Instant.now();
        InternalMessage readyMessage = create(messageDeliveryTime);
        long seconds = DispatcherThread.secondsUntilProcess(readyMessage, Instant.MAX, messageDeliveryTime.minusSeconds(1));
        assertTrue(seconds < 0);
    }

    @Test
    void shouldWaitForDeliveryTime() {
        Instant now = Instant.now();
        Instant nowWithDelay = now.minusSeconds(5 * 60);
        Instant deliveryTime = now.plusSeconds(1);
        Instant creationTime = deliveryTime.plusSeconds(60);
        InternalMessage readyMessage = create(deliveryTime, creationTime);
        long seconds = DispatcherThread.secondsUntilProcess(readyMessage, nowWithDelay, now);
        // difference between now and delivery time
        assertEquals(1, seconds);
    }

    @Test
    void shouldWaitForHoldTime() {
        Instant now = Instant.now();
        Instant nowWithDelay = now.minusSeconds(60);
        Instant deliveryTime = now.plusSeconds(5);
        Instant creationTime = nowWithDelay.plusSeconds(1);
        InternalMessage readyMessage = create(deliveryTime, creationTime);
        long seconds = DispatcherThread.secondsUntilProcess(readyMessage, nowWithDelay, now);
        // difference between nowWithDelay and hold time
        assertEquals(1, seconds);
    }

    @Value
    private static class R implements Consumer.Record {
        InternalMessage message;
    }
}