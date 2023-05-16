package com.platformatory.kafka.connect;

import io.netty.handler.codec.http.FullHttpRequest;

public interface Validator {
    public boolean validate(FullHttpRequest request);
}
