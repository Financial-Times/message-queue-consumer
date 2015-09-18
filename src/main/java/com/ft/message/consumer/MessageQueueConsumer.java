package com.ft.message.consumer;

import com.ft.api.util.transactionid.TransactionIdUtils;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.ft.messaging.standards.message.v1.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URI;
import java.util.Base64;

public class MessageQueueConsumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConsumer.class);
    private static final String TRANSACTION_ID = "transaction_id";

    private final MessageListener listener;
    private MessageQueueProxyService messageQueueProxyService;

    public MessageQueueConsumer(MessageQueueProxyService messageQueueProxyService, MessageListener listener) {
        this.listener = listener;
        this.messageQueueProxyService = messageQueueProxyService;
    }

    private void consume() {
        while (true) {
            URI consumerInstance = null;
            try {
                consumerInstance = messageQueueProxyService.createConsumerInstance();
                for (MessageRecord messageRecord : messageQueueProxyService.consumeMessages(consumerInstance)) {
                    Message message = null;
                    try {
                        message = Message.parse(messageRecord.getValue());
                        String transactionId = message.getCustomMessageHeader(TransactionIdUtils.TRANSACTION_ID_HEADER);
                        MDC.put(TRANSACTION_ID, "transaction_id=" + transactionId);
                        listener.onMessage(message, transactionId);
                    } catch (Throwable t) {
                        LOGGER.error(String.format("outcome=Exception message=\"Error while processing message [%s].\"", message), t);
                    } finally {
                        MDC.remove(TRANSACTION_ID);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("outcome=Exception message=\"Error while communicating with queue proxy.\"");
            }
            finally {
                if (consumerInstance != null) {
                    messageQueueProxyService.destroyConsumerInstance(consumerInstance);
                }
            }
        }
    }

    @Override
    public void run() {
        consume();
    }
}
