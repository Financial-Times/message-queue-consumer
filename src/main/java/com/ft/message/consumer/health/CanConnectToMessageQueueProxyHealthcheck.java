package com.ft.message.consumer.health;

import com.ft.message.consumer.config.HealtcheckConfiguration;
import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.CreateConsumerInstanceResponse;
import com.ft.platform.dropwizard.AdvancedHealthCheck;
import com.ft.platform.dropwizard.AdvancedResult;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
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
    private HealtcheckConfiguration healthcheckConfiguration;
    private Client proxyClient;

    public CanConnectToMessageQueueProxyHealthcheck(final Client proxyClient, final MessageQueueConsumerConfiguration configuration,
                                                    final HealtcheckConfiguration healthcheckConfiguration) {
        super(healthcheckConfiguration.getName());
        this.proxyClient = proxyClient;
        this.configuration = configuration;
        this.healthcheckConfiguration = healthcheckConfiguration;
    }

    @Override
    protected AdvancedResult checkAdvanced() throws Exception {
        ClientResponse clientResponse = null;
        try {
            URI uri = buildConsumerUri();
            clientResponse = getClientResponseForProxyConnection(uri);
            if (clientResponse.getStatus() != HTTP_RESPONSE_OK) {
                return reportUnhealthy(String.format("Unable to connect to queue proxy. %d", clientResponse.getStatus()));
            }

            URI consumerInstance = clientResponse.getEntity(CreateConsumerInstanceResponse.class).getBaseUri();
            URI messageReaderUri = buildMessageReaderUri(consumerInstance);
            clientResponse = getClientResponseForMessageConsumer(messageReaderUri);
            if (clientResponse.getStatus() != HTTP_RESPONSE_OK) {
                return reportUnhealthy(String.format("Unable to consume messages. Proxy returned %d", clientResponse.getStatus()));
            }

            clientResponse = deleteConsumerInstance(consumerInstance);
            if (clientResponse.getStatus() != HTTP_RESPONSE_NO_CONTENT) {
                return reportUnhealthy(String.format("Unable to destroy consumer instance. Proxy returned %d", clientResponse.getStatus()));
            }
        } catch (Throwable ex) {
            String message = getName() + ": " + "Exception during connecting to message queue proxy: " + ex.getLocalizedMessage();
            return reportUnhealthy(message);
        } finally {
            closeClientResponse(clientResponse);
        }
        return AdvancedResult.healthy("OK");
    }

    private void closeClientResponse(ClientResponse clientResponse) {
        if (clientResponse != null) {
            clientResponse.close();
        }
    }

    protected ClientResponse deleteConsumerInstance(URI consumerInstance) {
        URI proxyUri = UriBuilder.fromUri(configuration.getQueueProxyHost()).build();
        URI uri = UriBuilder.fromUri(consumerInstance)
                .host(proxyUri.getHost())
                .port(proxyUri.getPort())
                .build();
        return proxyClient.resource(uri)
                .header("Host", configuration.getQueue())
                .delete(ClientResponse.class);
    }

    protected ClientResponse getClientResponseForMessageConsumer(URI readMessageFromUri) {
        return proxyClient.resource(readMessageFromUri)
                .header("Host", configuration.getQueue())
                .header("Accept", "application/json")
                .get(ClientResponse.class);
    }

    protected ClientResponse getClientResponseForProxyConnection(URI uri) {
        return proxyClient.resource(uri)
                .header("Content-Type", "application/json")
                .header("Host", configuration.getQueue())
                .post(ClientResponse.class);
    }

    private URI buildMessageReaderUri(URI consumerUri) {
        URI proxyUri = UriBuilder.fromUri(configuration.getQueueProxyHost()).build();
        return UriBuilder.fromUri(consumerUri)
                .host(proxyUri.getHost())
                .port(proxyUri.getPort())
                .path("topics")
                .path(configuration.getTopicName())
                .build();
    }

    private URI buildConsumerUri() {
        return UriBuilder.fromUri(configuration.getQueueProxyHost())
                .path("consumers")
                .path(groupName)
                .build();
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
