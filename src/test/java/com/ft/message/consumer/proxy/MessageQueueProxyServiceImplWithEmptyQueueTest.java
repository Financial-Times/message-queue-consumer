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
public class MessageQueueProxyServiceImplWithEmptyQueueTest {

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
                        "",
                        8000, 1, "smallest"),
                client);
    }

    @Test
    public void testCreateConsumerInstance() throws Exception {

        final URI expectedUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);

        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\", \"auto.commit.enable\": \"false\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(CreateConsumerInstanceResponse.class)).thenReturn(new CreateConsumerInstanceResponse(expectedUri));

        URI actualConsumerInstanceUri = messageQueueProxyService.createConsumerInstance();

        assertThat(actualConsumerInstanceUri, is(equalTo(expectedUri)));

        verify(mockedBuilder).header(eq("Content-Type"), eq("application/json"));
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\", \"auto.commit.enable\": \"false\"}");
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testCreateConsumerInstanceWhenErrorOccurs() throws Exception {

        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage("Unable to create consumer instance. Proxy returned 500");

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"smallest\", \"auto.commit.enable\": \"false\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        URI actualConsumerInstanceUri = messageQueueProxyService.createConsumerInstance();

        assertThat(actualConsumerInstanceUri, nullValue());
        verify(mockedBuilder).header(eq("Content-Type"), eq("application/json"));
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testDestroyConsumerInstance() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(consumerUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(204);

        messageQueueProxyService.destroyConsumerInstance(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
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
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        messageQueueProxyService.destroyConsumerInstance(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testConsumeMessages() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("topics").path("CmsPublicationEvent").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(any(GenericType.class))).thenReturn(ImmutableList.of(new MessageRecord("myrecord".getBytes())));

        List<MessageRecord> actualMessageRecords = messageQueueProxyService.consumeMessages(consumerUri);

        assertThat(actualMessageRecords.get(0).getValue(), is(equalTo("myrecord".getBytes())));
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).header(eq("Accept"), eq("application/json"));
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
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        messageQueueProxyService.consumeMessages(consumerUri);
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).header(eq("Accept"), eq("application/json"));
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testCommitOffsets() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("offsets").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);

        messageQueueProxyService.commitOffsets(consumerUri);

        verify(mockedBuilder).post(ClientResponse.class);
        verify(mockedBuilder, never()).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();
    }

    @Test
    public void testCommitOffsetsWhenErrorOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage("Unable to commit offsets. Proxy returned 500");

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("offsets").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        messageQueueProxyService.commitOffsets(consumerUri);

        verify(mockedBuilder, never()).post(ClientResponse.class);
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();
    }
}