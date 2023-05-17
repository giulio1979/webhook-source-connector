package com.platformatory.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
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
//                            // TODO: Determine key
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
                // TODO: Determine key
                queue.add(new SourceRecord(sourcePartition, sourceOffset, topic, null, determineKey(request), null, jsonObject));
            } catch (JsonProcessingException e) {
                log.error("Could not convert request body to JSON - ", e);
            } finally {
                // Send a response
                String responseBody = "Good Bye!";
                sendHttpResponse(ctx, request, HttpResponseStatus.OK, responseBody);
            }
        } else {
            String responseBody = "Request validation failed";
            sendHttpResponse(ctx, request, HttpResponseStatus.BAD_REQUEST, responseBody);
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


    public DefaultRequestHandler(Validator validator, BlockingQueueFactory blockingQueueFactory, String dispatcherKey, String defaultTopic, String keyHeader) {
        this.validator = validator;
        this.blockingQueueFactory = blockingQueueFactory;
        this.dispatcherKey = dispatcherKey;
        this.defaultTopic = defaultTopic;
        this.keyHeader = keyHeader;
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

    public String determineKey(FullHttpRequest request) {
        // TODO: Support key extraction from a JSON path
        return request.headers().get(keyHeader);
    }
}
