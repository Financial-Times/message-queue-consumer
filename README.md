#message-queue-consumer

Library which does polling to consume messages from a message queue via an http proxy. Supports group and topic semantics.

Configuration
```
topicName - name of the topic from which to consume messages
groupName - consumer group
queueProxyHost - location of the http proxy(eg "http://localhost:8082")
queue - used for dynamic routing. This values will be passed on as an http Host header on all the requests
backoffPeriod - period in milliseconds for which the app will sleep before trying to consume messages 
	      - backoff is applied when queue is empty(last consume request returned no messages) or exception occured when trying to connect to the proxy	
```

How to use
```
#Add library as a dependency
#Configure consumer with the params explained above
#Add implementation for: com.ft.message.consumer.MessageListener 
```

