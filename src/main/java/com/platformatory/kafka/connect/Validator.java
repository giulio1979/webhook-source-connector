package com.platformatory.kafka.connect;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

public interface Validator {
    public boolean validate(HttpRequest request);
}
