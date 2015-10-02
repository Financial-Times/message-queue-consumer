package com.ft.message.consumer.config;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MessageQueueConsumerConfigurationTest {

    @Test
    public void testShouldUsedConfiguredBackoffPeriod() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 2000);
        assertThat(messageQueueConsumerConfiguration.getBackoffPeriod(), is(equalTo(2000)));
    }

    @Test
    public void testDefaultTo8sIfBackoffPeriodNotConfigured() throws Exception {
        MessageQueueConsumerConfiguration messageQueueConsumerConfiguration = new MessageQueueConsumerConfiguration("CmsPublicationEvent", "group1", "http://localhost:8082", "kafka", 0);
        assertThat(messageQueueConsumerConfiguration.getBackoffPeriod(), is(equalTo(8000)));
    }
}