package com.ft.message.consumer.proxy;

import com.ft.message.consumer.config.MessageQueueConsumerConfiguration;
import com.ft.message.consumer.proxy.model.ConsumerInstanceResponse;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueProxyServiceImplTest {
  private static final String NO_MSG = String.format(MessageQueueProxyService.MESSAGES_CONSUMED, 0);
    private static final String ONE_MSG = String.format(MessageQueueProxyService.MESSAGES_CONSUMED, 1);
    private static final String KAFKA_MESSAGE_CONTENT_TYPE = "application/vnd.kafka.v2+json";
    
    private MessageQueueProxyService messageQueueProxyService;
    private MessageQueueConsumerConfiguration configuration;

    @Mock
    private Client client;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        configuration = new MessageQueueConsumerConfiguration(
                "CmsPublicationEvent",
                "binaryIngester",
                "http://localhost:8082",
                "kafka",
                8000, 1, "earliest", false);
        messageQueueProxyService = new MessageQueueProxyServiceImpl(configuration, client);
    }

    @Test
    public void testCreateConsumerInstance() throws Exception {

        final URI expectedUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);

        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"earliest\", \"auto.commit.enable\": \"false\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(ConsumerInstanceResponse.class)).thenReturn(new ConsumerInstanceResponse(expectedUri));

        URI actualConsumerInstanceUri = messageQueueProxyService.createConsumerInstance();

        assertThat(actualConsumerInstanceUri, is(equalTo(expectedUri)));

        verify(mockedBuilder).header(eq("Content-Type"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).post(ClientResponse.class, "{\"auto.offset.reset\": \"earliest\", \"auto.commit.enable\": \"false\"}");
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo(NO_MSG));
    }

    @Test
    public void testCreateConsumerInstanceWithAutocommit() throws Exception {
        MessageQueueProxyService messageQueueProxyService = new MessageQueueProxyServiceImpl(
                new MessageQueueConsumerConfiguration(
                        "CmsPublicationEvent",
                        "binaryIngester",
                        "http://localhost:8082",
                        "kafka",
                        8000, 1, "earliest", true),
                client);

        final URI expectedUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);

        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"earliest\", \"auto.commit.enable\": \"true\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(ConsumerInstanceResponse.class)).thenReturn(new ConsumerInstanceResponse(expectedUri));

        URI actualConsumerInstanceUri = messageQueueProxyService.createConsumerInstance();

        assertThat(actualConsumerInstanceUri, is(equalTo(expectedUri)));

        verify(mockedBuilder).header(eq("Content-Type"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).post(ClientResponse.class, "{\"auto.offset.reset\": \"earliest\", \"auto.commit.enable\": \"true\"}");
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo(NO_MSG));
    }

    @Test
    public void testCreateConsumerInstanceWhenErrorOccurs() throws Exception {
        final String errorMessage = "Unable to create consumer instance. Proxy returned 500";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class, "{\"auto.offset.reset\": \"earliest\", \"auto.commit.enable\": \"false\"}")).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);
        
        try {
          messageQueueProxyService.createConsumerInstance();
        } finally {
          verify(mockedBuilder).header(eq("Content-Type"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          verify(mockedResponse, times(1)).close();
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
    }

    @Test
    public void testCreateConsumerInstanceWhenTimeoutOccurs() throws Exception {
        final String errorMessage = "Unable to create consumer instance. Proxy error.";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        when(mockedBuilder.post(eq(ClientResponse.class), anyString())).thenThrow(new ClientHandlerException("test timeout"));

        try {
          messageQueueProxyService.createConsumerInstance();
        } finally {
          verify(mockedBuilder).header(eq("Content-Type"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
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
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo("Consumer has been destroyed."));
    }

    @Test
    public void testDestroyConsumerInstanceShouldOverrideConsumerInstanceUri() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://kafka/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final URI expectedOverridenUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(expectedOverridenUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(204);

        messageQueueProxyService.destroyConsumerInstance(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(client).resource(expectedOverridenUri);
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo("Consumer has been destroyed."));
    }

    @Test
    public void testDestroyConsumerInstanceWhenErrorOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final String errorMessage = "Unable to destroy consumer instance. Proxy returned 500";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(consumerUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        try {
          messageQueueProxyService.destroyConsumerInstance(consumerUri);
        } finally {
          verify(mockedBuilder).delete(ClientResponse.class);
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          verify(mockedResponse, times(1)).close();
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
    }

    @Test
    public void testDestroyConsumerInstanceWhenTimeoutOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final String errorMessage = "Unable to destroy consumer instance. Proxy error.";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(consumerUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        when(mockedBuilder.delete(ClientResponse.class)).thenThrow(new ClientHandlerException("test timeout"));

        try {
          messageQueueProxyService.destroyConsumerInstance(consumerUri);
        } finally {
          verify(mockedBuilder).delete(ClientResponse.class);
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
    }

    @Test
    public void testSubscribeConsumerInstanceToTopic() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("subscription").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);

        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class, String.format("{\"topics\":[\"%s\"]}", configuration.getTopicName()))).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(204);
        when(mockedResponse.getEntity(ConsumerInstanceResponse.class)).thenReturn(new ConsumerInstanceResponse(consumerUri));

        messageQueueProxyService.subscribeConsumerInstanceToTopic(consumerUri);

        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).header(eq("Content-Type"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
        verify(mockedBuilder).post(ClientResponse.class, String.format("{\"topics\":[\"%s\"]}", configuration.getTopicName()));
        verify(mockedResponse, times(1)).close();

        assertThat(messageQueueProxyService.getStatus(), equalTo(NO_MSG));
    }

    @Test
    public void testDestroyConsumerInstanceSubscription() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("subscription").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.delete(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(204);

        messageQueueProxyService.destroyConsumerInstanceSubscription(consumerUri);

        verify(mockedBuilder).delete(ClientResponse.class);
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();

        assertThat(messageQueueProxyService.getStatus(), equalTo("Consumer has been destroyed."));
    }

    @Test
    public void testConsumeMessages() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("records").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(any(GenericType.class))).thenReturn(ImmutableList.of(new MessageRecord("myrecord".getBytes())));

        List<MessageRecord> actualMessageRecords = messageQueueProxyService.consumeMessages(consumerUri);

        assertThat(actualMessageRecords.get(0).getValue(), is(equalTo("myrecord".getBytes())));
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).header(eq("Accept"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo(ONE_MSG));
    }

    @Test
    public void testConsumeMessagesShouldOverrideConsumerInstanceUri() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final URI expectedOverridenUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(expectedOverridenUri).path("records").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);
        when(mockedResponse.getEntity(any(GenericType.class))).thenReturn(ImmutableList.of(new MessageRecord("myrecord".getBytes())));

        List<MessageRecord> actualMessageRecords = messageQueueProxyService.consumeMessages(consumerUri);

        assertThat(actualMessageRecords.get(0).getValue(), is(equalTo("myrecord".getBytes())));
        verify(client).resource(UriBuilder.fromUri(expectedOverridenUri).path("records").build());
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedBuilder).header(eq("Accept"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo(ONE_MSG));
    }

    @Test
    public void testConsumeMessagesWhenErrorOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final String errorMessage = "Unable to consume messages. Proxy returned 500";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("records").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.get(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);

        try {
          messageQueueProxyService.consumeMessages(consumerUri);
        } finally {
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          verify(mockedBuilder).header(eq("Accept"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
          verify(mockedResponse, times(1)).close();
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
    }

    @Test
    public void testConsumeMessagesWhenTimeoutOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final String errorMessage = "Unable to consume messages. Proxy error.";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("records").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        when(mockedBuilder.get(ClientResponse.class)).thenThrow(new ClientHandlerException("test timeout"));

        try {
          messageQueueProxyService.consumeMessages(consumerUri);
        } finally {
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          verify(mockedBuilder).header(eq("Accept"), eq(KAFKA_MESSAGE_CONTENT_TYPE));
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
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
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo(NO_MSG));
    }

    @Test
    public void testCommitOffsetsShouldOverrideConsumerInstanceUri() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://kafka/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final URI expectedOverridenUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1/offsets").build();

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(expectedOverridenUri)).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(200);

        messageQueueProxyService.commitOffsets(consumerUri);

        verify(mockedBuilder).post(ClientResponse.class);
        verify(mockedBuilder).header(eq("Host"), eq("kafka"));
        verify(client).resource(expectedOverridenUri);
        verify(mockedResponse, times(1)).close();
        
        assertThat(messageQueueProxyService.getStatus(), equalTo(NO_MSG));
    }

    @Test
    public void testCommitOffsetsWhenErrorOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final String errorMessage = "Unable to commit offsets. Proxy returned 500";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("offsets").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        final ClientResponse mockedResponse = mock(ClientResponse.class);
        when(mockedBuilder.post(ClientResponse.class)).thenReturn(mockedResponse);
        when(mockedResponse.getStatus()).thenReturn(500);
        
        try {
          messageQueueProxyService.commitOffsets(consumerUri);
        } finally {
          verify(mockedBuilder).post(ClientResponse.class);
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          verify(mockedResponse, times(1)).close();
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
    }

    @Test
    public void testCommitOffsetsWhenTimeoutOccurs() throws Exception {
        final URI consumerUri = UriBuilder.fromUri("http://localhost:8082/consumers/binaryIngester/instances/rest-consumer-1-1").build();
        final String errorMessage = "Unable to commit offsets. Proxy error.";
        expectedException.expect(QueueProxyServiceException.class);
        expectedException.expectMessage(errorMessage);

        final WebResource mockedWebResource = mock(WebResource.class);
        when(client.resource(UriBuilder.fromUri(consumerUri).path("offsets").build())).thenReturn(mockedWebResource);
        final WebResource.Builder mockedBuilder = mock(WebResource.Builder.class);
        when(mockedWebResource.getRequestBuilder()).thenReturn(mockedBuilder);
        when(mockedBuilder.post(ClientResponse.class)).thenThrow(new ClientHandlerException("test timeout"));

        try {
          messageQueueProxyService.commitOffsets(consumerUri);
        } finally {
          verify(mockedBuilder).post(ClientResponse.class);
          verify(mockedBuilder).header(eq("Host"), eq("kafka"));
          
          assertThat(messageQueueProxyService.getStatus(), equalTo(errorMessage));
        }
    }
}
