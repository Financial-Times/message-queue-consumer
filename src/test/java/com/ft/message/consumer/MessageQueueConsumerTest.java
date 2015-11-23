package com.ft.message.consumer;

import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.message.consumer.proxy.QueueProxyServiceException;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.ft.messaging.standards.message.v1.Message;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueConsumerTest {

    private static final String MESSAGE = "FTMSG/1.0\r\n" +
            "Message-Id: 557b0772-da28-47a4-8b4f-fc46cc5f8c32\r\n" +
            "Message-Timestamp: 2015-11-20T13:44:45.305Z\r\n" +
            "Message-Type: cms-content-published\r\n" +
            "Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub\r\n" +
            "Content-Type: application/json\r\n" +
            "X-Request-Id: SYNTHETIC-REQ-MON_b47A5AvpIr\r\n\r\n" +
            "{\"contentUri\":\"http://methode-image-model-transformer-iw-uk-p.svc.ft.com/image/model/30921224-0c0d-4522-9990-1ff0290d7908\"}";
    @Mock
    private MessageListener messageListener;
    @Mock
    private MessageQueueProxyService messageQueueProxyService;

    @Test
    public void testConsume() throws Exception {
        MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenReturn(ImmutableList.of(new MessageRecord(MESSAGE.getBytes())));
        when(messageListener.onMessage(Message.parse(MESSAGE.getBytes()), "SYNTHETIC-REQ-MON_b47A5AvpIr")).thenReturn(true);
        doNothing().when(messageQueueProxyService).commitOffsets(consumerInstance);

        messageQueueConsumer.consume();

        verify(messageQueueProxyService).consumeMessages(consumerInstance);
        verify(messageListener).onMessage(any(Message.class), eq("SYNTHETIC-REQ-MON_b47A5AvpIr"));
        verify(messageQueueProxyService).commitOffsets(consumerInstance);
        verify(messageQueueProxyService, never()).destroyConsumerInstance(consumerInstance);
    }

    @Test
    public void testConsumeShouldSkipInvalidMessage() throws Exception {
        MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenReturn(ImmutableList.of(new MessageRecord("Invalid Message".getBytes())));
        doNothing().when(messageQueueProxyService).destroyConsumerInstance(consumerInstance);

        messageQueueConsumer.consume();

        verify(messageQueueProxyService).consumeMessages(consumerInstance);
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService).commitOffsets(consumerInstance);
        verify(messageQueueProxyService, never()).destroyConsumerInstance(consumerInstance);
    }

    @Test
    public void testConsumeShouldDestroyConsumerAndBackOffInstanceWhenExceptionOccurs() throws Exception {
        MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1000);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenThrow(new QueueProxyServiceException("Could not reach the proxy"));
        doNothing().when(messageQueueProxyService).destroyConsumerInstance(consumerInstance);

        LocalTime timestamp = LocalTime.now();
        messageQueueConsumer.consume();

        assertThat(LocalTime.now().isAfter(timestamp.plus(1000, ChronoUnit.MILLIS)), is(true));
        verify(messageQueueProxyService, never()).commitOffsets(consumerInstance);
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService).destroyConsumerInstance(consumerInstance);
    }

    @Test
    public void testConsumeShouldNotDestroyAndBackOffWhenConsumerInstanceWhenNull() throws Exception {
        MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1000);

        when(messageQueueProxyService.createConsumerInstance()).thenThrow(new QueueProxyServiceException("Could not reach the proxy"));

        LocalTime timestamp = LocalTime.now();
        messageQueueConsumer.consume();

        assertThat(LocalTime.now().isAfter(timestamp.plus(1000, ChronoUnit.MILLIS)), is(true));
        verify(messageQueueProxyService, never()).commitOffsets(any(URI.class));
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService, never()).destroyConsumerInstance(any(URI.class));
    }

    @Test
    public void testConsumeShouldBackOffWhenQueueIsEmpty() throws Exception {
        MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1000);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenReturn(ImmutableList.<MessageRecord>of());

        LocalTime timestamp = LocalTime.now();
        messageQueueConsumer.consume();

        assertThat(LocalTime.now().isAfter(timestamp.plus(1000, ChronoUnit.MILLIS)), is(true));
        verify(messageQueueProxyService).consumeMessages(consumerInstance);
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService, never()).commitOffsets(consumerInstance);
        verify(messageQueueProxyService, never()).destroyConsumerInstance(consumerInstance);
    }

    @Test
    public void testConsumeShouldBackOffWhenUnableToDestroyConsumerInstance() throws Exception {
        MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1000);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenThrow(new QueueProxyServiceException("Could not reach the proxy"));
        doThrow(new QueueProxyServiceException("Could not reach the proxy")).when(messageQueueProxyService).destroyConsumerInstance(consumerInstance);

        LocalTime timestamp = LocalTime.now();
        messageQueueConsumer.consume();

        assertThat(LocalTime.now().isAfter(timestamp.plus(1000, ChronoUnit.MILLIS)), is(true));
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService, never()).commitOffsets(consumerInstance);
        verify(messageQueueProxyService).consumeMessages(consumerInstance);
        verify(messageQueueProxyService).destroyConsumerInstance(consumerInstance);
    }

    @Test
    public void testConsumeShouldDestroyConsumerInstanceWhenThreadInterruptedAndQueueNotEmpty() throws Exception {
        final MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1000);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenReturn(ImmutableList.of(new MessageRecord(MESSAGE.getBytes())));
        when(messageListener.onMessage(Message.parse(MESSAGE.getBytes()), "SYNTHETIC-REQ-MON_b47A5AvpIr")).thenReturn(true);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().interrupt();
                messageQueueConsumer.consume();
            }
        });

        executorService.awaitTermination(2, TimeUnit.SECONDS);
        verify(messageQueueProxyService, never()).commitOffsets(consumerInstance);
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService).destroyConsumerInstance(consumerInstance);
    }

    @Test
    public void testConsumeShouldDestroyConsumerInstanceWhenThreadInterruptedAndQueueEmpty() throws Exception {
        final MessageQueueConsumer messageQueueConsumer = new MessageQueueConsumer(messageQueueProxyService, messageListener, 1000);
        final URI consumerInstance = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        when(messageQueueProxyService.createConsumerInstance()).thenReturn(consumerInstance);
        when(messageQueueProxyService.consumeMessages(consumerInstance)).thenReturn(ImmutableList.<MessageRecord>of());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().interrupt();
                messageQueueConsumer.consume();
            }
        });

        executorService.awaitTermination(2, TimeUnit.SECONDS);
        verify(messageQueueProxyService, never()).commitOffsets(consumerInstance);
        verify(messageListener, never()).onMessage(any(Message.class), any(String.class));
        verify(messageQueueProxyService).destroyConsumerInstance(consumerInstance);
    }
}