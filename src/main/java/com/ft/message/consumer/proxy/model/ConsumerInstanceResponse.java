package com.ft.message.consumer.proxy.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerInstanceResponse {

    private URI baseUri;

    public ConsumerInstanceResponse(@JsonProperty("base_uri") URI baseUri) {
        this.baseUri = baseUri;
    }

    public URI getBaseUri() {
        return baseUri;
    }
}
