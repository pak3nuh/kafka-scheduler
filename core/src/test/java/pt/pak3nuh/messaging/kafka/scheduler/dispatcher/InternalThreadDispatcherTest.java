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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;
import static pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory.createInternalMessageStream;

class InternalThreadDispatcherTest {

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
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(delay, SchedulerTopic.Granularity.MINUTES), consumer, handler);
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
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(delay, SchedulerTopic.Granularity.MINUTES), consumer, handler);
        List<R> recordList = createInternalMessageStream(numberOfRecords, 0).map(R::new).collect(Collectors.toList());
        willReturn(recordList).given(consumer).poll();

        thread.doConsume();

        // all were paused
        verify(handler, new Times(0)).handle(any(InternalMessage.class));
        verify(consumer, new Times(numberOfRecords)).pause(any(), any());
    }

    @Test
    void shouldConsumeSomeAndPauseSome() {
        int delay = 1;
        DispatcherThread thread = new DispatcherThread(new SchedulerTopic(delay, SchedulerTopic.Granularity.MINUTES), consumer, handler);
        Stream<R> toConsume = createInternalMessageStream(2, -delay).map(R::new);
        Stream<R> toPause = createInternalMessageStream(2, 0).map(R::new);
        willReturn(Stream.concat(toConsume, toPause).collect(Collectors.toList())).given(consumer).poll();

        thread.doConsume();

        // all were paused
        verify(handler, new Times(2)).handle(any(InternalMessage.class));
        verify(consumer, new Times(2)).pause(any(), any());
    }

    @Value
    private static class R implements Consumer.Record {
        InternalMessage message;
    }
}