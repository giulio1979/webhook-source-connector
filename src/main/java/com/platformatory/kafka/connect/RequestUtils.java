package com.platformatory.kafka.connect;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RequestUtils {
    static final Logger log = LoggerFactory.getLogger(RequestUtils.class);
    static StringBuilder formatParams(HttpRequest request) {
        StringBuilder responseData = new StringBuilder();
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        if (!params.isEmpty()) {
            for (Entry<String, List<String>> p : params.entrySet()) {
                String key = p.getKey();
                List<String> vals = p.getValue();
                for (String val : vals) {
                    responseData.append("Parameter: ")
                            .append(key.toUpperCase())
                            .append(" = ")
                            .append(val.toUpperCase())
                            .append("\r\n");
                }
            }
            responseData.append("\r\n");
        }
        return responseData;
    }

    static StringBuilder formatBody(HttpContent httpContent) {
        log.info("Body {}", httpContent.content());
        StringBuilder responseData = new StringBuilder();
        ByteBuf content = httpContent.content();
        if (content.isReadable()) {
            responseData.append(content.toString(CharsetUtil.UTF_8)
                    .toUpperCase());
            responseData.append("\r\n");
        }
        return responseData;
    }

    static String topicResolver(HttpRequest request, String topicHeader) {
        return request.headers().get(topicHeader);
    }

    static StringBuilder evaluateDecoderResult(HttpObject o) {
        StringBuilder responseData = new StringBuilder();
        DecoderResult result = o.decoderResult();

        if (!result.isSuccess()) {
            responseData.append("..Decoder Failure: ");
            responseData.append(result.cause());
            responseData.append("\r\n");
        }

        return responseData;
    }

    static StringBuilder prepareLastResponse(String message, LastHttpContent trailer) {
        StringBuilder responseData = new StringBuilder();
        responseData.append(message+"\r\n");

        if (!trailer.trailingHeaders()
                .isEmpty()) {
            responseData.append("\r\n");
            for (CharSequence name : trailer.trailingHeaders()
                    .names()) {
                for (CharSequence value : trailer.trailingHeaders()
                        .getAll(name)) {
                    responseData.append("P.S. Trailing Header: ");
                    responseData.append(name)
                            .append(" = ")
                            .append(value)
                            .append("\r\n");
                }
            }
            responseData.append("\r\n");
        }
        return responseData;
    }

}