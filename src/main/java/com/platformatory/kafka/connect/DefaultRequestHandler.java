package com.platformatory.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
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

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@ChannelHandler.Sharable
public class DefaultRequestHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(DefaultRequestHandler.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private StringBuilder requestBodyBuilder = new StringBuilder();

    private boolean readingChunks = false;
    private HttpRequest request;
    private String topic;
    private final Validator validator;
    private final BlockingQueueFactory blockingQueueFactory;
    private final String dispatcherKey;
    private final String defaultTopic;
    StringBuilder responseData = new StringBuilder();

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        log.info("Message {}", msg);
        if (msg instanceof HttpRequest) {
            HttpRequest request = this.request = (HttpRequest) msg;
            log.info("Request {}", request.decoderResult());

            if (HttpUtil.is100ContinueExpected(request)) {
                writeResponse(ctx);
            }
            topic = extractQueueName(request);
            topic = topic == null ? defaultTopic : topic;
            requestBodyBuilder.setLength(0);
            responseData.setLength(0);
            responseData.append(RequestUtils.formatParams(request));
        }

        responseData.append(RequestUtils.evaluateDecoderResult(request));

        if (msg instanceof HttpContent) {
            HttpContent httpContent = (HttpContent) msg;
            log.info("Content {}", httpContent);
            responseData.append(RequestUtils.formatBody(httpContent));
            responseData.append(RequestUtils.evaluateDecoderResult(request));

            ByteBuf content = httpContent.content();
            if (content.isReadable()) {
                requestBodyBuilder.append(content.toString(CharsetUtil.UTF_8));
            }

            if (msg instanceof LastHttpContent) {
                String requestBody = requestBodyBuilder.toString();
                if (readingChunks) {
                    // End of chunked encoding.
                    requestBody = requestBody.trim();
                }
                if (!requestBody.isEmpty()) {
                    Object jsonObject = null;
                    if (validateRequest(request)) {
                        try {
                            jsonObject = mapper.readValue(requestBody, Object.class);
                            log.info("RequestJSON {}", jsonObject);
                            Map<String, ?> sourcePartition = new HashMap<>();
                            Map<String, ?> sourceOffset = new HashMap<>();
                            BlockingQueue<SourceRecord> queue = blockingQueueFactory.getOrCreateQueue(topic);
                            // TODO: Determine key
                            queue.add(new SourceRecord(sourcePartition, sourceOffset, topic, null, jsonObject));
                        } catch (JsonProcessingException e) {
                            log.error("Could not convert request body to JSON - ", e);
                        }
                    } else {
                        // TODO: Handle invalid request
                    }
                }
                LastHttpContent trailer = (LastHttpContent) msg;
                responseData.append(RequestUtils.prepareLastResponse(request, trailer));
                writeResponse(ctx, trailer, responseData);
            }
        }
    }

    private void writeResponse(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
        ctx.write(response);
    }

    private void writeResponse(ChannelHandlerContext ctx, LastHttpContent trailer, StringBuilder responseData) {
        boolean keepAlive = HttpUtil.isKeepAlive(request);

        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, ((HttpObject) trailer).decoderResult()
                .isSuccess() ? OK : BAD_REQUEST, Unpooled.copiedBuffer(responseData.toString(), CharsetUtil.UTF_8));

        httpResponse.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        if (keepAlive) {
            httpResponse.headers()
                    .setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content()
                            .readableBytes());
            httpResponse.headers()
                    .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        ctx.write(httpResponse);

        if (!keepAlive) {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }


    public DefaultRequestHandler(Validator validator, BlockingQueueFactory blockingQueueFactory, String dispatcherKey, String defaultTopic) {
        this.validator = validator;
        this.blockingQueueFactory = blockingQueueFactory;
        this.dispatcherKey = dispatcherKey;
        this.defaultTopic = defaultTopic;
    }
    private boolean validateRequest(HttpRequest request) {
        // Perform request validation using the configured validator
        return validator.validate(request);
    }

    private String extractQueueName(HttpRequest request) {
        HttpHeaders headers = request.headers();
        // TODO: Sanitize queue name to exclude commas
        return headers.get(dispatcherKey);
    }
}
