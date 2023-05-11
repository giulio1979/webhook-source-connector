package com.platformatory.kafka.connect;

import io.netty.handler.codec.http.HttpRequest;

public class DefaultValidator implements Validator{
    @Override
    public boolean validate(HttpRequest request) {
        return true;
    }
}
