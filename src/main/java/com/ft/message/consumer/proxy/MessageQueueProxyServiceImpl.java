package com.ft.message.consumer.proxy;

import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.CreateConsumerInstanceResponse;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;

public class MessageQueueProxyServiceImpl implements MessageQueueProxyService {

    private MessageQueueConsumerConfiguration configuration;
    private Client proxyClient;

    public MessageQueueProxyServiceImpl(MessageQueueConsumerConfiguration configuration, Client proxyClient) {
        this.configuration = configuration;
        this.proxyClient = proxyClient;
    }


    @Override
    public URI createConsumerInstance() {
        ClientResponse clientResponse = null;
        try {
            URI uri = UriBuilder.fromUri(configuration.getQueueProxyHost())
                    .path("consumers")
                    .path(configuration.getGroupName())
                    .build();

            WebResource.Builder builder = proxyClient.resource(uri).getRequestBuilder();
            builder.header("Content-Type", "application/json");
            if (queueIsNotEmpty()) {
                builder.header("Host", configuration.getQueue());
            }
            clientResponse = builder.post(ClientResponse.class, String.format("{\"auto.offset.reset\": \"%s\", \"auto.commit.enable\": \"true\"}", configuration.getOffsetReset()));

            if (clientResponse.getStatus() != 200) {
                throw new QueueProxyServiceException(String.format("Unable to create consumer instance. Proxy returned %d", clientResponse.getStatus()));
            }
            return clientResponse.getEntity(CreateConsumerInstanceResponse.class).getBaseUri();
        } finally {
            if (clientResponse != null) {
                clientResponse.close();
            }
        }
    }

    @Override
    public void destroyConsumerInstance(URI consumerInstance) {
        ClientResponse clientResponse = null;
        try {

            UriBuilder uriBuilder = UriBuilder.fromUri(consumerInstance);
            if (queueIsNotEmpty()) {
                addProxyPortAndHostInUri(uriBuilder);
            }
            URI uri = uriBuilder.build();

            WebResource.Builder builder = proxyClient.resource(uri).getRequestBuilder();
            if (queueIsNotEmpty()) {
                builder.header("Host", configuration.getQueue());
            }

            clientResponse = builder.delete(ClientResponse.class);

            if (clientResponse.getStatus() != 204) {
                throw new QueueProxyServiceException(String.format("Unable to destroy consumer instance. Proxy returned %d", clientResponse.getStatus()));
            }
        } finally {
            if (clientResponse != null) {
                clientResponse.close();
            }
        }
    }

    @Override
    public List<MessageRecord> consumeMessages(URI consumerInstance) {
        ClientResponse clientResponse = null;
        try {
            UriBuilder uriBuilder = UriBuilder.fromUri(consumerInstance).path("topics")
                    .path(configuration.getTopicName());

            if (queueIsNotEmpty()) {
                addProxyPortAndHostInUri(uriBuilder);
            }

            URI uri = uriBuilder.build();

            WebResource.Builder builder = proxyClient.resource(uri).getRequestBuilder();
            builder.header("Accept", "application/json");
            if (queueIsNotEmpty()) {
                builder.header("Host", configuration.getQueue());
            }
            clientResponse = builder.get(ClientResponse.class);

            if (clientResponse.getStatus() != 200) {
                throw new QueueProxyServiceException(String.format("Unable to consume messages. Proxy returned %d", clientResponse.getStatus()));
            }

            return clientResponse.getEntity(new GenericType<List<MessageRecord>>() {
            });
        } finally {
            if (clientResponse != null) {
                clientResponse.close();
            }
        }
    }

    @Override
    public void commitOffsets(URI consumerInstance) {
        ClientResponse clientResponse = null;
        try {

            UriBuilder uriBuilder = UriBuilder.fromUri(consumerInstance).path("offsets");
            if (queueIsNotEmpty()) {
                addProxyPortAndHostInUri(uriBuilder);
            }
            URI uri = uriBuilder.build();

            WebResource.Builder builder = proxyClient.resource(uri).getRequestBuilder();
            if (queueIsNotEmpty()) {
                builder.header("Host", configuration.getQueue());
            }

            clientResponse = builder.post(ClientResponse.class);

            if (clientResponse.getStatus() != 200) {
                throw new QueueProxyServiceException(String.format("Unable to commit offsets. Proxy returned %d", clientResponse.getStatus()));
            }
        } finally {
            if (clientResponse != null) {
                clientResponse.close();
            }
        }
    }

    private void addProxyPortAndHostInUri(UriBuilder uriBuilder) {
        URI proxyUri = UriBuilder.fromUri(configuration.getQueueProxyHost()).build();
        uriBuilder.host(proxyUri.getHost()).port(proxyUri.getPort());
    }

    private boolean queueIsNotEmpty() {
        return configuration.getQueue() != null && !configuration.getQueue().isEmpty();
    }
}
