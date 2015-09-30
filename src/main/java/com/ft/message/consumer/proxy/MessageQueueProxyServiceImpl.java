package com.ft.message.consumer.proxy;

import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.CreateConsumerInstanceResponse;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;

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
            clientResponse = proxyClient.resource(uri)
                    .header("Content-Type", "application/json")
                    .header("Host", configuration.getQueue())
                    .post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\"}");
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
            URI uri = UriBuilder.fromUri(consumerInstance)
                    .build();
            clientResponse = proxyClient.resource(uri)
                    .header("Host", configuration.getQueue())
                    .delete(ClientResponse.class);
            if (clientResponse.getStatus() != 204) {
                throw new QueueProxyServiceException(String.format("Unable to create consumer instance. Proxy returned %d", clientResponse.getStatus()));
            }
        } finally {
            if (clientResponse != null) {
                clientResponse.close();
            }
        }
    }

    @Override
    public List<MessageRecord> consumeMessages(URI consumerInstace) {
        ClientResponse clientResponse = null;
        try {
            URI uri = UriBuilder.fromUri(consumerInstace)
                    .path("topics")
                    .path(configuration.getTopicName())
                    .build();
            clientResponse = proxyClient.resource(uri)
                    .header("Host", configuration.getQueue())
                    .header("Accept", "application/json")
                    .get(ClientResponse.class);
            if (clientResponse.getStatus() != 200) {
                throw new QueueProxyServiceException(String.format("Unable to create consumer instance. Proxy returned %d", clientResponse.getStatus()));
            }

            return clientResponse.getEntity(new GenericType<List<MessageRecord>>(){});
        } finally {
            if (clientResponse != null) {
                clientResponse.close();
            }
        }
    }
}
