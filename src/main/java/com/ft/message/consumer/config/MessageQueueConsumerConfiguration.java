package com.ft.message.consumer.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageQueueConsumerConfiguration {

    private final String topicName;
    private final String groupName;
    private final String queueProxyHost;
    private final String queue;
    private final int queueProxyConnectionTimeout;

    public MessageQueueConsumerConfiguration(@JsonProperty("topicName") String topicName,
                                             @JsonProperty("groupName") String groupName,
                                             @JsonProperty("queueProxyHost") String queueProxyHost,
                                             @JsonProperty("queue") String queue,
                                             @JsonProperty("queueProxyConnectionTimeout") int queueProxyConnectionTimeout) {
        this.topicName = topicName;
        this.groupName = groupName;
        this.queueProxyHost = queueProxyHost;
        this.queue = queue;
        this.queueProxyConnectionTimeout = queueProxyConnectionTimeout;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getQueueProxyHost() {
        return queueProxyHost;
    }

    public int getQueueProxyConnectionTimeout() {
        return queueProxyConnectionTimeout;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getQueue() {
        return queue;
    }
}
