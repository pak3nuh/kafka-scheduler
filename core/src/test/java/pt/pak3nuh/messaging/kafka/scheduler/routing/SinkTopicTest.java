package pt.pak3nuh.messaging.kafka.scheduler.routing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pt.pak3nuh.messaging.kafka.scheduler.ClientMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.MessageFailureHandler;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;
import pt.pak3nuh.messaging.kafka.scheduler.producer.Producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory.create;
import static pt.pak3nuh.messaging.kafka.scheduler.mock.Mocking.mockStrict;

class SinkTopicTest {

    @Test
    void shouldDelegateToProducerOnSuccess() {
        Producer producer = mockStrict(Producer.class);
        willDoNothing().given(producer).send(any(), any(InternalMessage.class));
        InternalMessage internalMessage = create();

        new SinkTopic(producer, "destination", null, false).send(internalMessage);

        verify(producer).send("destination", internalMessage);
    }

    @Test
    void shouldDeliverClientMessage() {
        Producer producer = mockStrict(Producer.class);
        willDoNothing().given(producer).send(any(), any(ClientMessage.class));
        InternalMessage internalMessage = create();

        new SinkTopic(producer, "destination", null, true).send(internalMessage);

        verify(producer).send("destination", internalMessage.getClientMessage());
    }

    @Test
    void shouldThrowAndDelegateToHandlerOnFailure() {
        Producer producer = mockStrict(Producer.class);
        willThrow(new RuntimeException()).given(producer).send(any(), any(InternalMessage.class));
        MessageFailureHandler handler = mockStrict(MessageFailureHandler.class);
        InternalMessage internalMessage = create();
        willDoNothing().given(handler).handle(any(ClientMessage.class), any(SchedulerException.class));
        boolean thrown = false;

        try {
            new SinkTopic(producer, "destination", handler, false).send(internalMessage);
        } catch (SchedulerException e) {
            thrown = true;
        }

        Assertions.assertTrue(thrown);
        verify(handler).handle(eq(internalMessage.getClientMessage()), any());
    }
}