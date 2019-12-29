package pt.pak3nuh.messaging.kafka.scheduler.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessage;
import pt.pak3nuh.messaging.kafka.scheduler.InternalMessageFactory;
import pt.pak3nuh.messaging.kafka.scheduler.SchedulerException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;
import static pt.pak3nuh.messaging.kafka.scheduler.mock.Mocking.mockStrict;

class ProducerImplTest {

    @Test
    void souldDelegateToTheProducerOnSend() {
        org.apache.kafka.clients.producer.Producer<String, InternalMessage> producer =
                mockStrict(org.apache.kafka.clients.producer.Producer.class);
        willReturn(CompletableFuture.completedFuture(null)).given(producer).send(any());
        InternalMessage internalMessage = InternalMessageFactory.createInternalMessage();

        new ProducerImpl(producer).send("topic", internalMessage);
        ArgumentCaptor<ProducerRecord<String, InternalMessage>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

        verify(producer).send(captor.capture());
        assertEquals("topic", captor.getValue().topic());
        assertSame(internalMessage, captor.getValue().value());
    }

    @Test
    void shouldCloseProducer() {
        org.apache.kafka.clients.producer.Producer<String, InternalMessage> producer =
                mockStrict(org.apache.kafka.clients.producer.Producer.class);
        willDoNothing().given(producer).close();

        new ProducerImpl(producer).close();

        verify(producer).close();
    }

    @Test
    void shouldThrowExceptionOnProducerError() throws ExecutionException, InterruptedException {
        org.apache.kafka.clients.producer.Producer<String, InternalMessage> producer =
                mockStrict(org.apache.kafka.clients.producer.Producer.class);
        Future<RecordMetadata> future = mockStrict(Future.class);
        willThrow(InterruptedException.class).given(future).get();
        willReturn(future).given(producer).send(any());

        assertThrows(SchedulerException.class,
                () -> new ProducerImpl(producer).send("", InternalMessageFactory.createInternalMessage()));
    }


}