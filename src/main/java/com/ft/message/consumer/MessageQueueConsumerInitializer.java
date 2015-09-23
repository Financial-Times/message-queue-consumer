package com.ft.message.consumer;

import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.message.consumer.proxy.MessageQueueProxyServiceImpl;
import com.sun.jersey.api.client.Client;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageQueueConsumerInitializer implements Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConsumerInitializer.class);

    private final MessageQueueConsumer messageQueueConsumer;
    private final Client queueProxyClient;

    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener,
                                           Client queueProxyClient) {
        this.queueProxyClient = queueProxyClient;
        MessageQueueProxyService messageQueueProxyService = new MessageQueueProxyServiceImpl(consumerConfiguration, queueProxyClient);
        messageQueueConsumer = new MessageQueueConsumer(
                messageQueueProxyService,
                listener,
                consumerConfiguration.getBackoffPeriod());
    }

    @Override
    public void start() throws Exception {
		ExecutorService startupExecutor = Executors.newSingleThreadExecutor();
		startupExecutor.submit(messageQueueConsumer);
		LOGGER.info("Lazy start for ReceivedMessagesHandler executed");
    }

    @Override
    public void stop() throws Exception {
		queueProxyClient.destroy();
    }

}
