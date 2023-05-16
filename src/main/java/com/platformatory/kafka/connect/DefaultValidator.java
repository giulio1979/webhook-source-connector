package com.platformatory.kafka.connect;

import io.netty.handler.codec.http.FullHttpRequest;

public class DefaultValidator implements Validator{
    @Override
    public boolean validate(FullHttpRequest request) {
        return true;
    }
}
