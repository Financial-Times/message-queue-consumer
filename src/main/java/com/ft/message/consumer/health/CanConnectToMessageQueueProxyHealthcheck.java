package com.ft.message.consumer.health;

import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.CreateConsumerInstanceResponse;
import com.ft.platform.dropwizard.AdvancedHealthCheck;
import com.ft.platform.dropwizard.AdvancedResult;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class CanConnectToMessageQueueProxyHealthcheck extends AdvancedHealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(CanConnectToMessageQueueProxyHealthcheck.class);
    private static final String groupName = "healthcheck";
    private static final int HTTP_RESPONSE_OK = 200;
    private static final int HTTP_RESPONSE_NO_CONTENT = 204;

    private MessageQueueConsumerConfiguration configuration;
    private HealthcheckConfiguration healthcheckConfiguration;
    private Client proxyClient;

    public CanConnectToMessageQueueProxyHealthcheck(final Client proxyClient, final MessageQueueConsumerConfiguration configuration,
                                                    final HealthcheckConfiguration healthcheckConfiguration) {
        super(healthcheckConfiguration.getName());
        this.proxyClient = proxyClient;
        this.configuration = configuration;
        this.healthcheckConfiguration = healthcheckConfiguration;
    }

    @Override
    protected AdvancedResult checkAdvanced() throws Exception {
        ClientResponse clientResponseToCreateConsumer = null;
        ClientResponse clientResponseToCheckTopic = null;
        ClientResponse clientResponseToCloseConsumer = null;
        try {
            URI uri = buildConsumerUri();
            clientResponseToCreateConsumer = getClientResponseForProxyConnection(uri);
            if (clientResponseToCreateConsumer.getStatus() != HTTP_RESPONSE_OK) {
                return reportUnhealthy(String.format("Unable to connect to queue proxy. %d", clientResponseToCreateConsumer.getStatus()));
            }

            URI consumerInstance = clientResponseToCreateConsumer.getEntity(CreateConsumerInstanceResponse.class).getBaseUri();
            URI messageReaderUri = buildMessageReaderUri(consumerInstance);
            clientResponseToCheckTopic = getClientResponseForMessageConsumer(messageReaderUri);
            if (clientResponseToCheckTopic.getStatus() != HTTP_RESPONSE_OK) {
                return reportUnhealthy(String.format("Unable to consume messages. Proxy returned %d", clientResponseToCheckTopic.getStatus()));
            }

            clientResponseToCloseConsumer = deleteConsumerInstance(consumerInstance);
            if (clientResponseToCloseConsumer.getStatus() != HTTP_RESPONSE_NO_CONTENT) {
                return reportUnhealthy(String.format("Unable to destroy consumer instance. Proxy returned %d", clientResponseToCloseConsumer.getStatus()));
            }
        } catch (Throwable ex) {
            String message = getName() + ": " + "Exception during connecting to message queue proxy: " + ex.getLocalizedMessage();
            return reportUnhealthy(message);
        } finally {
            closeClientResponse(clientResponseToCreateConsumer);
            closeClientResponse(clientResponseToCheckTopic);
            closeClientResponse(clientResponseToCloseConsumer);
        }
        return AdvancedResult.healthy("OK");
    }

    private void closeClientResponse(ClientResponse clientResponse) {
        if (clientResponse != null) {
            clientResponse.close();
        }
    }

    protected ClientResponse deleteConsumerInstance(URI consumerInstance) {
        UriBuilder uriBuilder = UriBuilder.fromUri(consumerInstance);
        if (queueIsNotEmpty()) {
            addProxyPortAndHostInUri(uriBuilder);
        }
        URI uri = uriBuilder.build();

        WebResource.Builder builder = proxyClient.resource(uri).getRequestBuilder();
        if (queueIsNotEmpty()) {
            builder.header("Host", configuration.getQueue());
        }
        return builder.delete(ClientResponse.class);
    }

    protected ClientResponse getClientResponseForMessageConsumer(URI readMessageFromUri) {
        WebResource.Builder builder = proxyClient.resource(readMessageFromUri).getRequestBuilder();
        builder.header("Accept", "application/json");
        if (queueIsNotEmpty()) {
            builder.header("Host", configuration.getQueue());
        }
        return builder.get(ClientResponse.class);
    }

    protected ClientResponse getClientResponseForProxyConnection(URI uri) {
        WebResource.Builder builder = proxyClient.resource(uri).getRequestBuilder();
        builder.header("Content-Type", "application/json");
        if (queueIsNotEmpty()) {
            builder.header("Host", configuration.getQueue());
        }
        return builder.post(ClientResponse.class);
    }

    private URI buildMessageReaderUri(URI consumerUri) {
        UriBuilder uriBuilder = UriBuilder.fromUri(consumerUri).path("topics").path(configuration.getTopicName());
        if (queueIsNotEmpty()) {
            addProxyPortAndHostInUri(uriBuilder);
        }
        return uriBuilder.build();
    }

    private URI buildConsumerUri() {
        return UriBuilder.fromUri(configuration.getQueueProxyHost())
                .path("consumers")
                .path(groupName)
                .build();
    }


    private void addProxyPortAndHostInUri(UriBuilder uriBuilder) {
        URI proxyUri = UriBuilder.fromUri(configuration.getQueueProxyHost()).build();
        uriBuilder.host(proxyUri.getHost()).port(proxyUri.getPort());
    }

    private boolean queueIsNotEmpty() {
        return configuration.getQueue() != null && !configuration.getQueue().isEmpty();
    }

    private AdvancedResult reportUnhealthy(String message) {
        logger.warn(getName() + ": " + message);
        return AdvancedResult.error(this, message);
    }

    @Override
    protected int severity() {
        return healthcheckConfiguration.getSeverity();
    }

    @Override
    protected String businessImpact() {
        return healthcheckConfiguration.getBusinessImpact();
    }

    @Override
    protected String technicalSummary() {
        return healthcheckConfiguration.getTechnicalSummary();
    }

    @Override
    protected String panicGuideUrl() {
        return healthcheckConfiguration.getPanicGuideUrl();
    }

    @Override
    public String getName() {
        return healthcheckConfiguration.getName();
    }
}
