package com.ft.message.consumer.proxy;

import com.ft.message.consumer.proxy.model.MessageRecord;

import java.net.URI;
import java.util.List;

public interface MessageQueueProxyService {

    URI createConsumerInstance();

    void destroyConsumerInstance(URI consumerInstance);

    List<MessageRecord> consumeMessages(URI consumerInstance);

    void commitOffsets(URI consumerInstance);
}
