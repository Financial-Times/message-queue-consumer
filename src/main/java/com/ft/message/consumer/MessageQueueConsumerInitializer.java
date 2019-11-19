package com.ft.message.consumer;

import com.codahale.metrics.MetricRegistry;
import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.health.PassiveMessageQueueProxyConsumerHealthcheck;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.message.consumer.proxy.MessageQueueProxyServiceImpl;
import com.ft.platform.dropwizard.AdvancedHealthCheck;
import com.sun.jersey.api.client.Client;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.ft.message.consumer.QueueProxyClientSingleton.*;

public class MessageQueueConsumerInitializer implements Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConsumerInitializer.class);

    private final MessageQueueConsumerConfiguration messageQueueConsumerConfiguration;
    private final MessageListener messageListener;
    private Client queueProxyClient;
    private Environment env;
    private JerseyClientConfiguration jerseyConfig;
    private String queueProxyClientName;
    private final MessageQueueProxyService messageQueueProxyService;
    final ExecutorService startupExecutor;
    
    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener,
                                           Client queueProxyClient) {
      
        this(consumerConfiguration, listener,
                getQueueProxyClientSingleInstance(queueProxyClient, null, null, null),
                null);
    }

    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener,
                                           Client queueProxyClient,
                                           ExecutorService executorService) {
      
        this.queueProxyClient = getQueueProxyClientSingleInstance(queueProxyClient, env, jerseyConfig, queueProxyClientName);
        this.messageQueueConsumerConfiguration = consumerConfiguration;
        this.messageListener = listener;
        this.startupExecutor = executorService != null ?
            executorService : Executors.newFixedThreadPool(consumerConfiguration.getStreamCount());
        this.messageQueueProxyService =
            new MessageQueueProxyServiceImpl(messageQueueConsumerConfiguration, queueProxyClient);
    }

    public MessageQueueConsumerInitializer(MessageQueueConsumerConfiguration consumerConfiguration,
                                           MessageListener listener,
                                           Client queueProxyClient,
                                           ExecutorService executorService,
                                           Environment env,
                                           JerseyClientConfiguration jerseyConfig,
                                           String queueProxyClientName) {

        this.env = env;
        this.jerseyConfig = jerseyConfig;
        this.queueProxyClientName = queueProxyClientName;
        this.queueProxyClient = getQueueProxyClientSingleInstance(queueProxyClient, env, jerseyConfig, queueProxyClientName);
        this.messageQueueConsumerConfiguration = consumerConfiguration;
        this.messageListener = listener;
        this.startupExecutor = executorService != null ?
                executorService : Executors.newFixedThreadPool(consumerConfiguration.getStreamCount());
        this.messageQueueProxyService =
                new MessageQueueProxyServiceImpl(messageQueueConsumerConfiguration, this.queueProxyClient, env, jerseyConfig, queueProxyClientName);
    }

    @Override
    public void start() throws Exception {
        for (int i = 0; i < messageQueueConsumerConfiguration.getStreamCount(); i++) {
            startupExecutor.submit(new InfiniteStreamHandler(new MessageQueueConsumer(
                    messageQueueProxyService,
                    messageListener,
                    messageQueueConsumerConfiguration.getBackoffPeriod(),
                    messageQueueConsumerConfiguration.isAutoCommit())));
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
    
    public AdvancedHealthCheck buildPassiveConsumerHealthcheck(
        HealthcheckConfiguration healthcheckConfiguration, MetricRegistry metrics) {
      
      return new PassiveMessageQueueProxyConsumerHealthcheck(
          healthcheckConfiguration, messageQueueProxyService, metrics);
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
