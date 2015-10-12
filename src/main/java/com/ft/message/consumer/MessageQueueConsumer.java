package com.ft.message.consumer;

import com.ft.api.util.transactionid.TransactionIdUtils;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.ft.messaging.standards.message.v1.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MessageQueueConsumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConsumer.class);
    private static final String TRANSACTION_ID = "transaction_id";

    private final MessageListener listener;
    private MessageQueueProxyService messageQueueProxyService;
    private int backoffPeriod;

    public MessageQueueConsumer(MessageQueueProxyService messageQueueProxyService, MessageListener listener, int backoffPeriod) {
        this.listener = listener;
        this.messageQueueProxyService = messageQueueProxyService;
        this.backoffPeriod = backoffPeriod;
    }

    private void consume() {
        while (true) {
            URI consumerInstance = null;
            List<MessageRecord> messageRecords = null;
            try {
                consumerInstance = messageQueueProxyService.createConsumerInstance();
                messageRecords = messageQueueProxyService.consumeMessages(consumerInstance);
                for (MessageRecord messageRecord : messageRecords) {
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
                LOGGER.error("outcome=Exception message=\"Error while communicating with queue proxy.\"", t);
            } finally {
                try {
                    if (consumerInstance != null) {
                        messageQueueProxyService.destroyConsumerInstance(consumerInstance);
                    }
                    if (messageRecords == null || messageRecords.isEmpty()) {
                        TimeUnit.MILLISECONDS.sleep(backoffPeriod);
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted while sleeping", e);
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    LOGGER.warn("outcome=Exception message=\"Error while destroying consumer instance.\"", t);
                }
            }
        }
    }

    @Override
    public void run() {
        consume();
    }
}
