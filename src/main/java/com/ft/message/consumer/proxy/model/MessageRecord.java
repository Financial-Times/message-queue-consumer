package com.ft.message.consumer.proxy.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageRecord {

    private final byte[] value;

    public MessageRecord(@JsonProperty("value") byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }
}
