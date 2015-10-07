package com.ft.message.consumer.health;

import com.ft.message.consumer.config.HealtcheckParameters;
import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.CreateConsumerInstanceResponse;
import com.ft.platform.dropwizard.AdvancedResult;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.net.URISyntaxException;

import static com.ft.dropwizard.matcher.AdvancedHealthCheckResult.unhealthy;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CanConnectToMessageQueueProxyHealthcheckTest {

    private static final String topicName = "topicName";
    private static final String groupName = "groupName";
    private static final String proxyHost = "http://localhost:8082";

    private HealtcheckParameters healthcheckParameters;
    private MessageQueueConsumerConfiguration consumerConfiguration;
    private CanConnectToMessageQueueProxyHealthcheck healthcheck;

    @Mock
    private Client mockClient;

    @Before
    public void setUp() {
        healthcheckParameters = new HealtcheckParameters("kafka-proxy", 2, "business impact text", "tech summary", "panic guide url");
        consumerConfiguration = new MessageQueueConsumerConfiguration(topicName, groupName, proxyHost, "kafka", 8000);
        healthcheck = new CanConnectToMessageQueueProxyHealthcheck(mockClient, consumerConfiguration, healthcheckParameters);
    }

    @Test
    public void shouldReturnErrorWhenResponseIsNull() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);
        ClientResponse clientResponse = null;
        doReturn(clientResponse).when(healthCheck).getClientResponseForProxyConnection((URI) any());

        assertThat(healthcheck.checkAdvanced(), is(unhealthy(healthcheckParameters.getName() + ": " + "Exception during connecting to kafka proxy: ")));
    }

    @Test
    public void shouldReturnErrorWhenHostIsNotReachable() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);
        ClientResponse mockClientResponse = mockClientResponseForProxyConnection(healthCheck, 500);

        AdvancedResult expectedHealthCheckResult = AdvancedResult.error(healthCheck, String.format("Unable to connect to queue proxy. %d", mockClientResponse.getStatus()));
        AdvancedResult actualHealthCheckResult = healthCheck.checkAdvanced();

        assertThat(actualHealthCheckResult.status(), is(equalTo(expectedHealthCheckResult.status())));
        assertThat(actualHealthCheckResult.checkOutput(), containsString(expectedHealthCheckResult.checkOutput()));
    }


    @Test
    public void shouldReturnErrorWhenTopicIsNotFound() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);

        mockClientResponseForProxyConnection(healthCheck, 200);
        ClientResponse mockClientResponseForMessageConsuming = mockClientResponseForMessageConsuming(healthCheck, 500);

        AdvancedResult expectedHealthCheckResult = AdvancedResult.error(healthCheck, String.format("Unable to consume messages. Proxy returned %d",
                mockClientResponseForMessageConsuming.getStatus()));
        AdvancedResult actualHealthCheckResult = healthCheck.checkAdvanced();

        assertThat(actualHealthCheckResult.status(), is(equalTo(expectedHealthCheckResult.status())));
        assertThat(actualHealthCheckResult.checkOutput(), containsString(expectedHealthCheckResult.checkOutput()));
    }

    @Test
    public void shouldReturnHealthyWhenConnectionEstablishedAndMessagesCanBeConsumed() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);

        mockClientResponseForProxyConnection(healthCheck, 200);
        mockClientResponseForMessageConsuming(healthCheck, 200);
        mockClientResponseAtConsumerDestroy(healthCheck, 204);

        AdvancedResult expectedHealthCheckResult = AdvancedResult.healthy("OK");
        AdvancedResult actualHealthCheckResult = healthCheck.checkAdvanced();

        assertThat(actualHealthCheckResult.status(), is(equalTo(expectedHealthCheckResult.status())));
        assertThat(actualHealthCheckResult.checkOutput(), containsString(expectedHealthCheckResult.checkOutput()));
    }

    private void mockClientResponseAtConsumerDestroy(CanConnectToMessageQueueProxyHealthcheck healthCheck, int statusCode) {
        ClientResponse mockClientResponse = mock(ClientResponse.class);
        doReturn(mockClientResponse).when(healthCheck).deleteConsumerInstance((URI) any());
        doNothing().when(mockClientResponse).close();
        when(mockClientResponse.getStatus()).thenReturn(statusCode);
    }

    private ClientResponse mockClientResponseForMessageConsuming(CanConnectToMessageQueueProxyHealthcheck healthCheck, int statusCode) {
        ClientResponse mockClientResponse = mock(ClientResponse.class);
        when(mockClientResponse.getStatus()).thenReturn(statusCode);
        doReturn(mockClientResponse).when(healthCheck).getClientResponseForMessageConsumer((URI) any());
        doNothing().when(mockClientResponse).close();
        return mockClientResponse;
    }

    private ClientResponse mockClientResponseForProxyConnection(CanConnectToMessageQueueProxyHealthcheck healthCheck, int statusCode) throws URISyntaxException {
        ClientResponse mockClientResponse = mock(ClientResponse.class);
        when(mockClientResponse.getStatus()).thenReturn(statusCode);
        doReturn(mockClientResponse).when(healthCheck).getClientResponseForProxyConnection((URI) any());
        doNothing().when(mockClientResponse).close();

        if (statusCode == 200) {
            CreateConsumerInstanceResponse response = mock(CreateConsumerInstanceResponse.class);
            when(mockClientResponse.getEntity(CreateConsumerInstanceResponse.class)).thenReturn(response);
            doReturn(new URI("uri")).when(response).getBaseUri();
        }

        return mockClientResponse;
    }
}