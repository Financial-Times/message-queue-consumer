package com.ft.message.consumer.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

public class MessageQueueConsumerConfiguration {

    private final String topicName;
    private final String groupName;
    private final String queueProxyHost;
    private final String queue;
    private final int backoffPeriod;
    private final int streamCount;
    private final String offsetReset;
    private final boolean autoCommit;

    public MessageQueueConsumerConfiguration(@JsonProperty("topicName") String topicName,
                                             @JsonProperty("groupName") String groupName,
                                             @JsonProperty("queueProxyHost") String queueProxyHost,
                                             @JsonProperty("queue") String queue,
                                             @JsonProperty("backoffPeriod") int backoffPeriod,
                                             @JsonProperty("streamCount") int streamCount,
                                             @JsonProperty("offsetReset") String offsetReset,
                                             @JsonProperty("autoCommit") boolean autoCommit) {
        this.topicName = topicName;
        this.groupName = groupName;
        this.queueProxyHost = queueProxyHost;
        this.queue = queue;
        this.autoCommit = autoCommit;
        this.backoffPeriod = backoffPeriod == 0? 8000 : backoffPeriod;
        this.streamCount = streamCount == 0? 1 : streamCount;
        // Any value of offsetReset other than "latest", "earliest" or "none" causes a Kafka consumer exception
        this.offsetReset = Strings.isNullOrEmpty(offsetReset) ? "latest" : offsetReset;
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

    public boolean isAutoCommit() {
        return autoCommit;
    }
}
