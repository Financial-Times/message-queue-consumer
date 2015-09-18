package com.ft.message.consumer.proxy.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateConsumerInstanceResponse {

    private URI baseUri;

    public CreateConsumerInstanceResponse(@JsonProperty("base_uri") URI baseUri) {
        this.baseUri = baseUri;
    }

    public URI getBaseUri() {
        return baseUri;
    }
}
