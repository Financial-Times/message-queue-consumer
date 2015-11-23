package com.ft.message.consumer.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageQueueConsumerConfiguration {

    private final String topicName;
    private final String groupName;
    private final String queueProxyHost;
    private final String queue;
    private final int backoffPeriod;
    private final int streamCount;
    private final String offsetReset;

    public MessageQueueConsumerConfiguration(@JsonProperty("topicName") String topicName,
                                             @JsonProperty("groupName") String groupName,
                                             @JsonProperty("queueProxyHost") String queueProxyHost,
                                             @JsonProperty("queue") String queue,
                                             @JsonProperty("backoffPeriod") int backoffPeriod,
                                             @JsonProperty("streamCount") int streamCount,
                                             @JsonProperty("offsetReset") String offsetReset) {
        this.topicName = topicName;
        this.groupName = groupName;
        this.queueProxyHost = queueProxyHost;
        this.queue = queue;
        this.backoffPeriod = backoffPeriod == 0? 8000 : backoffPeriod;
        this.streamCount = streamCount == 0? 1 : streamCount;
        this.offsetReset = offsetReset == null? "largest" : offsetReset;
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

    public int getStreamCount() {
        return streamCount;
    }

    public String getOffsetReset() {
        return offsetReset;
    }
}
