package com.ft.message.consumer.proxy;


public class QueueProxyServiceException extends RuntimeException {
    public QueueProxyServiceException(String message) {
        super(message);
    }
    
    public QueueProxyServiceException(String message, Throwable cause) {
      super(message, cause);
  }
}
