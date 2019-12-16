# message-queue-consumer

A library that is an intermediary to Confluent Kafka REST Proxy. It implements v2 of the API: <https://docs.confluent.io/current/kafka-rest/api.html#api-v2>.

It supports the following settings:


```
topicName - name of the topic from which to consume messages
groupName - consumer group
queueProxyHost - location of the http proxy(eg "http://localhost:8082")
queue - used for dynamic routing. This values will be passed on as an http Host header on all the requests
backoffPeriod - period in milliseconds for which the app will sleep before trying to consume messages 
        - backoff is applied when queue is empty(last consume request returned no messages) or exception occurred when trying to connect to the proxy
autoCommit - boolean flag which configures autoCommit when consuming messages. If true offsets are committed if the consume request to the proxy returns 200.
           - if false offsets are manually committed after the batch of messages are processed
           - because of the proxy limitations the recommendations are to use autocommit true for topics with small messages
offsetReset - possible values are "none", "earliest" and "latest"
            - earliest: start processing all messages from the beginning of a Kafka topic
            - latest: start processing messages which are produced after the consumer started
            - none: throw exception to the consumer if no previous offset is found for the consumer's group
            - because of the proxy limitations the recommendations are to use "latest" unless you have a very good reasons not to
            - using "earliest" will impact the memory usage of the proxy
streamCount - number of threads to use for processing messages
            - each thread will create a new proxy consumer instance which will be assigned to different kafka partition(s)
```
The library expects a jersey http client to be passed in. Make sure the client you provide supports overriding http Host header.

## How to use

1. Add library as a dependency
1. Configure consumer with the params explained above
1. Add implementation for: `com.ft.message.consumer.MessageListener`
