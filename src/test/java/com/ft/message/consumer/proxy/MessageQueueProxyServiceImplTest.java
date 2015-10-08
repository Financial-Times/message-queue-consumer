package com.ft.message.consumer.proxy;

import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.CreateConsumerInstanceResponse;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueProxyServiceImplTest {

    private MessageQueueProxyService messageQueueProxyService;

    @Mock
    private Client client;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        messageQueueProxyService = new MessageQueueProxyServiceImpl(
                new MessageQueueConsumerConfiguration(
                        "CmsPublicationEvent",
                        "binaryIngester",
                        "http://localhost:8082",
                        "kafka",
                        8000),
                client);
    }

    @Test
    public void testCreateConsumerInstance() throws Exception {

        final URI expectedUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Content-Type"), eq("application/json"))).thenReturn(mockedBuilder);
        final WebResource.Builder anotherMockedBuilder = mock(WebResource.Builder.class);
        when(mockedBuilder.header(eq("Host"), eq("kafka"))).thenReturn(anotherMockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(anotherMockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\", \"auto.commit.enable\": \"true\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(CreateConsumerInstanceResponse.class)).thenReturn(new CreateConsumerInstanceResponse(expectedUri));

        URI actualConsumerInstanceUri = messageQueueProxyService.createConsumerInstance();

        assertThat(actualConsumerInstanceUri, is(equalTo(expectedUri)));
        verify(anotherMockedBuilder).post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\", \"auto.commit.enable\": \"true\"}");
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testCreateConsumerInstanceWhenErrorOccurs() throws Exception {

        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage("Unable to create consumer instance. Proxy returned 500");

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Content-Type"), eq("application/json"))).thenReturn(mockedBuilder);
        final WebResource.Builder anotherMockedBuilder = mock(WebResource.Builder.class);
        when(mockedBuilder.header(eq("Host"), eq("kafka"))).thenReturn(anotherMockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(anotherMockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\", \"auto.commit.enable\": \"true\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        URI actualConsumerInstanceUri = messageQueueProxyService.createConsumerInstance();

        assertThat(actualConsumerInstanceUri, nullValue());
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testDestroyConsumerInstance() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(consumerUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Host"), eq("kafka"))).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(204);

        messageQueueProxyService.destroyConsumerInstance(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testDestroyConsumerInstanceShouldOverrideConsumerInstanceUri() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://kafka/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final URI expectedOverridenUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(expectedOverridenUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Host"), eq("kafka"))).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(204);

        messageQueueProxyService.destroyConsumerInstance(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(client).resource(expectedOverridenUri);
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testDestroyConsumerInstanceWhenErrorOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage("Unable to destroy consumer instance. Proxy returned 500");

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(consumerUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Host"), eq("kafka"))).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        messageQueueProxyService.destroyConsumerInstance(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testConsumeMessages() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("topics").path("CmsPublicationEvent").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Host"), eq("kafka"))).thenReturn(mockedBuilder);
        final WebResource.Builder anotherMockedBuilder = mock(WebResource.Builder.class);
        when(mockedBuilder.header(eq("Accept"), eq("application/json"))).thenReturn(anotherMockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(anotherMockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(any(GenericType.class))).thenReturn(ImmutableList.of(new MessageRecord("myrecord".getBytes())));

        List<MessageRecord> actualMessageRecords = messageQueueProxyService.consumeMessages(consumerUri);

        assertThat(actualMessageRecords.get(0).getValue(), is(equalTo("myrecord".getBytes())));
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testConsumeMessagesShouldOverrideConsumerInstanceUri() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final URI expectedOverridenUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(expectedOverridenUri).path("topics").path("CmsPublicationEvent").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Host"), eq("kafka"))).thenReturn(mockedBuilder);
        final WebResource.Builder anotherMockedBuilder = mock(WebResource.Builder.class);
        when(mockedBuilder.header(eq("Accept"), eq("application/json"))).thenReturn(anotherMockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(anotherMockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(any(GenericType.class))).thenReturn(ImmutableList.of(new MessageRecord("myrecord".getBytes())));

        List<MessageRecord> actualMessageRecords = messageQueueProxyService.consumeMessages(consumerUri);

        assertThat(actualMessageRecords.get(0).getValue(), is(equalTo("myrecord".getBytes())));
        verify(client).resource(UriBuilder.fromUri(expectedOverridenUri).path("topics").path("CmsPublicationEvent").build());
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testConsumeMessagesWhenErrorOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage("Unable to consume messages. Proxy returned 500");

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("topics").path("CmsPublicationEvent").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.header(eq("Host"), eq("kafka"))).thenReturn(mockedBuilder);
        final WebResource.Builder anotherMockedBuilder = mock(WebResource.Builder.class);
        when(mockedBuilder.header(eq("Accept"), eq("application/json"))).thenReturn(anotherMockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(anotherMockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        messageQueueProxyService.consumeMessages(consumerUri);

        verify(mockedResponse, times(1)).close();
    }
}