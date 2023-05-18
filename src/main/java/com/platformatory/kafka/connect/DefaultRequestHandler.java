package com.platformatory.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ChannelHandler.Sharable
public class DefaultRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger log = LoggerFactory.getLogger(DefaultRequestHandler.class);

    private static final String VALID_TOPIC_NAME_REGEX = "[^a-z0-9\\._\\-]+";
    private final ObjectMapper mapper = new ObjectMapper();
    private StringBuilder requestBodyBuilder = new StringBuilder();

    private boolean readingChunks = false;
    private HttpRequest request;
    private String topic;
    private final Validator validator;
    private final BlockingQueueFactory blockingQueueFactory;
    private final String dispatcherKey;
    private final String defaultTopic;
    private final String keyHeader;
    private final String keyJSONPath;
    private Map<String, Function<FullHttpRequest, Map.Entry<HttpResponseStatus, String>>> routes;
    StringBuilder responseData = new StringBuilder();

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

//    @Override
//    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
//        log.info("Message {}", msg);
//        if (msg instanceof HttpRequest) {
//            HttpRequest request = this.request = (HttpRequest) msg;
//            log.info("Request {}", request.decoderResult());
//
//            if (HttpUtil.is100ContinueExpected(request)) {
//                writeResponse(ctx);
//            }
//            topic = extractQueueName(request);
//            topic = topic == null ? defaultTopic : topic;
//            requestBodyBuilder.setLength(0);
//            responseData.setLength(0);
//            responseData.append(RequestUtils.formatParams(request));
//        }
//
//        responseData.append(RequestUtils.evaluateDecoderResult(request));
//
//        if (msg instanceof HttpContent) {
//            HttpContent httpContent = (HttpContent) msg;
//            log.info("Content {}", httpContent);
//            responseData.append(RequestUtils.formatBody(httpContent));
//            responseData.append(RequestUtils.evaluateDecoderResult(request));
//
//            ByteBuf content = httpContent.content();
//            if (content.isReadable()) {
//                requestBodyBuilder.append(content.toString(CharsetUtil.UTF_8));
//            }
//
//            if (msg instanceof LastHttpContent) {
//                String requestBody = requestBodyBuilder.toString();
//                if (readingChunks) {
//                    // End of chunked encoding.
//                    requestBody = requestBody.trim();
//                }
//                if (!requestBody.isEmpty()) {
//                    Object jsonObject = null;
//                    if (validateRequest(request, requestBody)) {
//                        try {
//                            jsonObject = mapper.readValue(requestBody, Object.class);
//                            log.info("RequestJSON {}", jsonObject);
//                            Map<String, ?> sourcePartition = new HashMap<>();
//                            Map<String, ?> sourceOffset = new HashMap<>();
//                            BlockingQueue<SourceRecord> queue = blockingQueueFactory.getOrCreateQueue(topic);
//                            queue.add(new SourceRecord(sourcePartition, sourceOffset, topic, null, jsonObject));
//                        } catch (JsonProcessingException e) {
//                            log.error("Could not convert request body to JSON - ", e);
//                        } finally {
//                            LastHttpContent trailer = (LastHttpContent) msg;
//                            responseData.append(RequestUtils.prepareLastResponse("Good bye!", trailer));
//                            writeResponse(ctx, ((HttpObject) trailer).decoderResult()
//                                    .isSuccess() ? OK : BAD_REQUEST, responseData);
//                        }
//                    } else {
//                        LastHttpContent trailer = (LastHttpContent) msg;
//                        responseData.append(RequestUtils.prepareLastResponse("Request validation failed", trailer));
//                        writeResponse(ctx, BAD_REQUEST, responseData);
//                    }
//                }
//
//            }
//        }
//    }
//
//    private void writeResponse(ChannelHandlerContext ctx) {
//        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
//        ctx.write(response);
//    }
//
//    private void writeResponse(ChannelHandlerContext ctx, HttpResponseStatus responseStatus, StringBuilder responseData) {
//        boolean keepAlive = HttpUtil.isKeepAlive(request);
//
//        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, responseStatus, Unpooled.copiedBuffer(responseData.toString(), CharsetUtil.UTF_8));
//
//        httpResponse.headers()
//                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
//
//        if (keepAlive) {
//            httpResponse.headers()
//                    .setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content()
//                            .readableBytes());
//            httpResponse.headers()
//                    .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
//        }
//
//        ctx.write(httpResponse);
//
//        if (!keepAlive) {
//            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
//                    .addListener(ChannelFutureListener.CLOSE);
//        }
//    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        Function<FullHttpRequest, Map.Entry<HttpResponseStatus, String>> handler = routes.get(uri);
        if (handler != null) {
            Map.Entry<HttpResponseStatus, String> responseContent = handler.apply(request);
            sendHttpResponse(ctx, request, responseContent.getKey(), responseContent.getValue());
        } else {
            sendHttpResponse(ctx, request, HttpResponseStatus.NOT_FOUND, "404 - Not Found");
        }
    }

    private void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status,
                                  String content) {
        // Build the response object
        ByteBuf buffer = ctx.alloc().buffer();
        buffer.writeBytes(content.getBytes(CharsetUtil.UTF_8));
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buffer);

        // Set the content type header
        HttpHeaders headers = response.headers();
        headers.set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        // Add the content length header
        headers.set(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());

        // Set the keep-alive header
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        // Write and flush the response
        ctx.writeAndFlush(response);

        // Close the connection if keep-alive is not requested
        if (!keepAlive) {
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    private Map.Entry<HttpResponseStatus, String> handleHealthCheckRequest(FullHttpRequest request) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        String strDate = sdf.format(now);
        return new AbstractMap.SimpleImmutableEntry <>(
                HttpResponseStatus.OK,
                "Healthy at " + strDate
        );
    }

    private Map.Entry<HttpResponseStatus, String> handleWebhookRequest(FullHttpRequest request) {
        topic = extractQueueName(request);
        topic = topic == null ? defaultTopic : topic;
        // Process the incoming request
        String requestBody = request.content().toString(CharsetUtil.UTF_8);
        log.info("Received request body: " + requestBody);
        if (validateRequest(request)) {
            Object jsonObject = null;
            try {
                jsonObject = mapper.readValue(requestBody, Object.class);
                Map<String, ?> sourcePartition = new HashMap<>();
                Map<String, ?> sourceOffset = new HashMap<>();
                BlockingQueue<SourceRecord> queue = blockingQueueFactory.getOrCreateQueue(topic);
                queue.add(new SourceRecord(sourcePartition, sourceOffset, topic, null, determineKey(request), null, jsonObject));
                return new AbstractMap.SimpleImmutableEntry <>(
                        HttpResponseStatus.OK,
                        "Good Bye!"
                );
            } catch (JsonProcessingException e) {
                log.error("Could not convert request body to JSON - ", e);
                return new AbstractMap.SimpleImmutableEntry <>(
                        HttpResponseStatus.BAD_REQUEST,
                        "Could not convert request body to JSON"
                );
            }
        } else {
            String responseBody = "";
            return new AbstractMap.SimpleImmutableEntry <>(
                    HttpResponseStatus.BAD_REQUEST,
                    "Request validation failed"
            );
        }
    }




    public DefaultRequestHandler(Validator validator, BlockingQueueFactory blockingQueueFactory, String dispatcherKey, String defaultTopic, String keyHeader, String keyJSONPath) {
        this.validator = validator;
        this.blockingQueueFactory = blockingQueueFactory;
        this.dispatcherKey = dispatcherKey;
        this.defaultTopic = defaultTopic;
        this.keyHeader = keyHeader;
        this.keyJSONPath = keyJSONPath;
        this.routes = new HashMap<>();
        this.routes.put("/", this::handleWebhookRequest);
        this.routes.put("/health", this::handleHealthCheckRequest);
    }
    private boolean validateRequest(FullHttpRequest request) {
        // Perform request validation using the configured validator
        return validator.validate(request);
    }

    private String extractQueueName(HttpRequest request) {
        HttpHeaders headers = request.headers();
        String queueName = headers.get(dispatcherKey);
        if(queueName == null || queueName.length() == 0) {
            return queueName;
        }
        Pattern pattern = Pattern.compile(VALID_TOPIC_NAME_REGEX);
        Matcher matcher = pattern.matcher(queueName);

        return matcher.replaceAll("_");
    }

    public Object determineKey(FullHttpRequest request) {
        if (keyJSONPath != null && !keyJSONPath.isEmpty()) {
            String requestBodyJSONString = request.content().toString(StandardCharsets.UTF_8);
            try {
                return JsonPath.read(requestBodyJSONString, keyJSONPath);
            } catch (PathNotFoundException e) {
                return null;
            }
        }
        return keyHeader == null ? null : request.headers().get(keyHeader);
    }
}
