package com.ft.message.consumer;

import com.ft.api.util.transactionid.TransactionIdUtils;
import com.ft.message.consumer.proxy.MessageQueueProxyService;
import com.ft.message.consumer.proxy.QueueProxyServiceException;
import com.ft.message.consumer.proxy.model.MessageRecord;
import com.ft.messaging.standards.message.v1.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MessageQueueConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConsumer.class);
    private static final String TRANSACTION_ID = "transaction_id";

    private final MessageListener listener;
    private MessageQueueProxyService messageQueueProxyService;
    private int backoffPeriod;
    private URI consumerInstance;
    private boolean autoCommit;

    public MessageQueueConsumer(MessageQueueProxyService messageQueueProxyService, MessageListener listener, int backoffPeriod, boolean autoCommit) {
        this.listener = listener;
        this.messageQueueProxyService = messageQueueProxyService;
        this.backoffPeriod = backoffPeriod;
        this.autoCommit = autoCommit;
    }

    public void consume() {
        try {
            if (consumerInstance == null) {
                consumerInstance = messageQueueProxyService.createConsumerInstance();
            }
            List<MessageRecord> messageRecords = messageQueueProxyService.consumeMessages(consumerInstance);
            if (messageRecords == null || messageRecords.isEmpty()) {
                backOff();
            } else {
                handleMessages(messageRecords);
                if(!autoCommit) {
                    messageQueueProxyService.commitOffsets(consumerInstance);
                }
            }
            if(Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
        } catch (QueueProxyServiceException e) {
          resetConsumer("Error while communicating with queue proxy.", e);
        } catch (Throwable t) {
          resetConsumer(t.getMessage(), t);
        }
    }
    
    private void resetConsumer(String reason, Throwable t) {
      String msg = reason;
      try {
          if (consumerInstance != null) {
              messageQueueProxyService.destroyConsumerInstance(consumerInstance);
          }
      } catch (Throwable t1) {
        msg += "; Error while destroying consumer instance.";
      } finally {
        LOGGER.error(String.format("outcome=Exception message=\"%s\"", msg), t);
          consumerInstance = null;
          backOff();
      }
    }
    
    private void backOff() {
        try {
            TimeUnit.MILLISECONDS.sleep(backoffPeriod);
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while sleeping", e);
            Thread.currentThread().interrupt();
        }
    }

    private void handleMessages(List<MessageRecord> messageRecords) throws InterruptedException{
        for (MessageRecord messageRecord : messageRecords) {
            if(Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
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
    }
}
