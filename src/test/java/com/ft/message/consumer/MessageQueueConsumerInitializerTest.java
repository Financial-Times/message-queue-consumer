package com.ft.message.consumer;

import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.platform.dropwizard.AdvancedHealthCheck;
import com.sun.jersey.api.client.Client;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class MessageQueueConsumerInitializerTest {

    @Mock
    private MessageListener messageListener;
    @Mock
    private MessageQueueProxyService messageQueueProxyService;
    @Mock
    private MessageQueueConsumerConfiguration messageQueueConsumerConfiguration;
    @Mock
    private Client client;
    @Mock
    private ExecutorService executorService;
    @Mock
    private MessageQueueConsumer messageQueueConsumer;
    @Mock
    private HealthcheckConfiguration healthcheckConfiguration;

    @Test
    public void testStop() throws Exception {
        new MessageQueueConsumerInitializer(messageQueueConsumerConfiguration, messageListener, client, executorService, healthcheckConfiguration).stop();

        verify(client).destroy();
        verify(executorService).shutdownNow();
        verify(executorService).awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testStart() throws Exception {
        when(messageQueueConsumerConfiguration.getStreamCount()).thenReturn(5);
        new MessageQueueConsumerInitializer(messageQueueConsumerConfiguration, messageListener, client, executorService, healthcheckConfiguration).start();

        verify(executorService, times(5)).submit(any(MessageQueueConsumerInitializer.InfiniteStreamHandler.class));
    }

    @Test
    public void testInfiniteStreamHandlerShouldTerminateWhenInterrupted() throws Exception {
        doNothing().when(messageQueueConsumer).consume();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new MessageQueueConsumerInitializer.InfiniteStreamHandler(messageQueueConsumer));
        executor.shutdownNow();

        assertThat(executor.awaitTermination(1, TimeUnit.SECONDS), is(true));
    }
    
    @Test
    public void thatHealthcheckIsSupplied() {
      MessageQueueConsumerInitializer initializer = new MessageQueueConsumerInitializer(
          messageQueueConsumerConfiguration, messageListener, client,
          executorService, healthcheckConfiguration);
      
      AdvancedHealthCheck actual = initializer.getPassiveConsumerHealthcheck();
      assertThat(actual, notNullValue());
    }
    
    @Test(expected = NullPointerException.class)
    public void thatHealthcheckRequiresHealthcheckConfiguration() {
      when(messageQueueConsumerConfiguration.getStreamCount()).thenReturn(1);
      
      MessageQueueConsumerInitializer initializer = new MessageQueueConsumerInitializer(
          messageQueueConsumerConfiguration, messageListener, client);
      
      initializer.getPassiveConsumerHealthcheck();
    }
}