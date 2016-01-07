package com.ft.message.consumer;

import com.ft.messaging.standards.message.v1.Message;

public interface MessageListener {

	boolean onMessage(Message message, String transactionId);

}
