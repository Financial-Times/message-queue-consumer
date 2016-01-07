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
import java.util.concurrent.TimeUnit;

public class MessageQueueConsumerInitializer implements Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConsumerInitializer.class);

    private final MessageQueueConsumerConfiguration messageQueueConsumerConfiguration;
    private final MessageListener messageListener;
    private final Client queueProxyClient;
    final ExecutorService startupExecutor;

    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener,
                                           Client queueProxyClient) {
        this.queueProxyClient = queueProxyClient;
        this.messageQueueConsumerConfiguration = consumerConfiguration;
        this.messageListener = listener;
        startupExecutor = Executors.newFixedThreadPool(consumerConfiguration.getStreamCount());
    }

    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener,
                                           Client queueProxyClient,
                                           ExecutorService executorService) {
        this.queueProxyClient = queueProxyClient;
        this.messageQueueConsumerConfiguration = consumerConfiguration;
        this.messageListener = listener;
        this.startupExecutor = executorService;
    }

    @Override
    public void start() throws Exception {
        MessageQueueProxyService messageQueueProxyService = new MessageQueueProxyServiceImpl(messageQueueConsumerConfiguration, queueProxyClient);
        for (int i = 0; i < messageQueueConsumerConfiguration.getStreamCount(); i++) {
            startupExecutor.submit(new InfiniteStreamHandler(new MessageQueueConsumer(
                    messageQueueProxyService,
                    messageListener,
                    messageQueueConsumerConfiguration.getBackoffPeriod())));
        }
        LOGGER.info("Lazy start for ReceivedMessagesHandler executed");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Gracefully shutting down");
        queueProxyClient.destroy();
        startupExecutor.shutdownNow();
        startupExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    final static class InfiniteStreamHandler implements Runnable {

        private final MessageQueueConsumer messageQueueConsumer;

        public InfiniteStreamHandler(MessageQueueConsumer messageQueueConsumer) {
            this.messageQueueConsumer = messageQueueConsumer;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                messageQueueConsumer.consume();
            }
            LOGGER.info("Exited gracefully;");
        }
    }

}
