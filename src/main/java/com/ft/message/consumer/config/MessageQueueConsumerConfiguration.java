package com.ft.message.consumer.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageQueueConsumerConfiguration {

    private final String topicName;
    private final String groupName;
    private final String queueProxyHost;
    private final String queue;
    private final int backoffPeriod;

    public MessageQueueConsumerConfiguration(@JsonProperty("topicName") String topicName,
                                             @JsonProperty("groupName") String groupName,
                                             @JsonProperty("queueProxyHost") String queueProxyHost,
                                             @JsonProperty("queue") String queue,
                                             @JsonProperty("backoffPeriod") int backoffPeriod) {
        this.topicName = topicName;
        this.groupName = groupName;
        this.queueProxyHost = queueProxyHost;
        this.queue = queue;
        this.backoffPeriod = backoffPeriod == 0? 8000 : backoffPeriod;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getQueueProxyHost() {
        return queueProxyHost;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getQueue() {
        return queue;
    }

    public int getBackoffPeriod() {
        return backoffPeriod;
    }
}
