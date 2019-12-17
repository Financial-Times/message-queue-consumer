package com.ft.message.consumer.health;

import com.ft.message.consumer.config.HealthcheckConfiguration;
import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.ConsumerInstanceResponse;
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

    private static final int HTTP_RESPONSE_OK = 200;
    private static final int HTTP_RESPONSE_NO_CONTENT = 204;
    private static final int HTTP_RESPONSE_INTERNAL_SERVER_ERROR = 500;

    private HealthcheckConfiguration healtcheckConfiguration;
    private CanConnectToMessageQueueProxyHealthcheck healthcheck;

    @Mock
    private Client mockClient;

    @Before
    public void setUp() {
        healtcheckConfiguration = new HealthcheckConfiguration("kafka-proxy", 2, "business impact text", "tech summary", "panic guide url");
        MessageQueueConsumerConfiguration consumerConfiguration = new MessageQueueConsumerConfiguration(topicName, groupName, proxyHost, "kafka", 8000, 1, "earliest", false);
        healthcheck = new CanConnectToMessageQueueProxyHealthcheck(mockClient, consumerConfiguration, healtcheckConfiguration);
    }

    @Test
    public void shouldReturnErrorWhenResponseIsNull() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);
        ClientResponse clientResponse = null;
        doReturn(clientResponse).when(healthCheck).getClientResponseForProxyConnection((URI) any());

        assertThat(healthCheck.checkAdvanced(), is(unhealthy(healtcheckConfiguration.getName() + ": " + "Exception during connecting to message queue proxy: ")));
    }

    @Test
    public void shouldReturnErrorWhenHostIsNotReachable() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);
        ClientResponse mockClientResponse = mockClientResponseForProxyConnection(healthCheck, HTTP_RESPONSE_INTERNAL_SERVER_ERROR);

        AdvancedResult expectedHealthCheckResult = AdvancedResult.error(healthCheck, String.format("Unable to connect to queue proxy. %d", mockClientResponse.getStatus()));
        AdvancedResult actualHealthCheckResult = healthCheck.checkAdvanced();

        assertThat(actualHealthCheckResult.status(), is(equalTo(expectedHealthCheckResult.status())));
        assertThat(actualHealthCheckResult.checkOutput(), containsString(expectedHealthCheckResult.checkOutput()));
    }


    @Test
    public void shouldReturnErrorWhenTopicIsNotFound() throws Exception {
        CanConnectToMessageQueueProxyHealthcheck healthCheck = spy(healthcheck);

        mockClientResponseForProxyConnection(healthCheck, HTTP_RESPONSE_OK);
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

        mockClientResponseForProxyConnection(healthCheck, HTTP_RESPONSE_OK);
        mockClientResponseForMessageConsuming(healthCheck, HTTP_RESPONSE_OK);
        mockClientResponseAtConsumerDestroy(healthCheck, HTTP_RESPONSE_NO_CONTENT);

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

        if (statusCode == HTTP_RESPONSE_OK) {
            ConsumerInstanceResponse response = mock(ConsumerInstanceResponse.class);
            when(mockClientResponse.getEntity(ConsumerInstanceResponse.class)).thenReturn(response);
            doReturn(new URI("uri")).when(response).getBaseUri();
        }

        return mockClientResponse;
    }
}