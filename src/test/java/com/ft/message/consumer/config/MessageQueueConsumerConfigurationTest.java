package com.ft.message.consumer.config;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MessageQueueConsumerConfigurationTest {

    @Test
    public void testShouldUsedConfiguredBackoffPeriod() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 1, "smallest");
        assertThat(messageQueueConsumerConfiguration.getBackoffPeriod(), is(equalTo(2000)));
    }

    @Test
    public void testDefaultTo8sIfBackoffPeriodNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0, 1, "smallest");
        assertThat(messageQueueConsumerConfiguration.getBackoffPeriod(), is(equalTo(8000)));
    }

    @Test
    public void testShouldUsedConfiguredStreamCount() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 4, "smallest");
        assertThat(messageQueueConsumerConfiguration.getStreamCount(), is(equalTo(4)));
    }

    @Test
    public void testDefaultTo1IfStreamCountNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0, 0, "smallest");
        assertThat(messageQueueConsumerConfiguration.getStreamCount(), is(equalTo(1)));
    }

    @Test
    public void testShouldUsedConfiguredOffsetReset() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 1, "smallest");
        assertThat(messageQueueConsumerConfiguration.getOffsetReset(), is(equalTo("smallest")));
    }

    @Test
    public void testDefaultToLargestIfOffsetResetNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0, 1, null);
        assertThat(messageQueueConsumerConfiguration.getOffsetReset(), is(equalTo("largest")));
    }
}