package com.ft.message.consumer;

import com.ft.jerseyhttpwrapper.ResilientClientBuilder;
import com.sun.jersey.api.client.Client;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.setup.Environment;

public class QueueProxyClientSingleton {
    private static Client queueProxyClientSingleInstance;
    private QueueProxyClientSingleton() {}

    public static Client getQueueProxyClientSingleInstance(Client queueProxyClient, Environment env, JerseyClientConfiguration jerseyConfig, String name) {
        if (queueProxyClient != null ){
            return  queueProxyClient;
        }
        if(queueProxyClientSingleInstance == null) {
            synchronized(QueueProxyClientSingleton.class) {
                queueProxyClientSingleInstance = getMessagingClient(env, jerseyConfig, name);
            }
        }
        return queueProxyClientSingleInstance;
    }

    private static Client getMessagingClient(Environment environment, JerseyClientConfiguration jerseyConfig, String name) {
        jerseyConfig.setGzipEnabled(false);
        jerseyConfig.setGzipEnabledForRequests(false);
        return ResilientClientBuilder.in(environment)
                .using(jerseyConfig)
                .usingDNS()
                .named(name)
                .build();
    }
}
