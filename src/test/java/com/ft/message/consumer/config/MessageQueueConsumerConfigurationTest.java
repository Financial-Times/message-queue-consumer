package com.ft.message.consumer.config;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MessageQueueConsumerConfigurationTest {

    @Test
    public void testShouldUseConfiguredBackoffPeriod() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 1, "earliest", false);
        assertThat(messageQueueConsumerConfiguration.getBackoffPeriod(), is(equalTo(2000)));
    }

    @Test
    public void testDefaultTo8sIfBackoffPeriodNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0, 1, "earliest", false);
        assertThat(messageQueueConsumerConfiguration.getBackoffPeriod(), is(equalTo(8000)));
    }

    @Test
    public void testShouldUseConfiguredStreamCount() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 4, "earliest", false);
        assertThat(messageQueueConsumerConfiguration.getStreamCount(), is(equalTo(4)));
    }

    @Test
    public void testDefaultTo1IfStreamCountNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0, 0, "earliest", false);
        assertThat(messageQueueConsumerConfiguration.getStreamCount(), is(equalTo(1)));
    }

    @Test
    public void testShouldUseConfiguredOffsetReset() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 1, "earliest", false);
        assertThat(messageQueueConsumerConfiguration.getOffsetReset(), is(equalTo("earliest")));
    }

    @Test
    public void testDefaultToLatestIfOffsetResetNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0, 1, null, false);
        assertThat(messageQueueConsumerConfiguration.getOffsetReset(), is(equalTo("latest")));
    }

    @Test
    public void testShouldUseConfiguredAutoCommit() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000, 1, "earliest", true);
        assertThat(messageQueueConsumerConfiguration.isAutoCommit(), is(equalTo(true)));
    }
}