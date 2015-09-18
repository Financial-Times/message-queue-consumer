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

    private final Client queueProxyClient;
    private final MessageQueueConsumer messageQueueConsumer;

    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener) {
        queueProxyClient = Client.create();
        MessageQueueProxyService messageQueueProxyService = new MessageQueueProxyServiceImpl(consumerConfiguration, queueProxyClient);
        messageQueueConsumer = new MessageQueueConsumer(
                messageQueueProxyService,
                listener);
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
