package com.platformatory.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@Description("This is a description of this connector and will show up in the documentation")
@DocumentationImportant("This is a important information that will show up in the documentation.")
@DocumentationTip("This is a tip that will show up in the documentation.")
@Title("Super Source Connector") //This is the display name that will show up in the documentation.
@DocumentationNote("This is a note that will show up in the documentation")
public class WebhookSourceConnector extends SourceConnector {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  private static final Logger log = LoggerFactory.getLogger(WebhookSourceConnector.class);
  private WebhookSourceConnectorConfig config;
  private QueueMonitor queueMonitor;
  public static BlockingQueueFactory blockingQueueFactory;
  private int port;
  private String topicHeader;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> map) {
    if(config==null) {
      log.info("Connector started");
      config = new WebhookSourceConnectorConfig(map);
      blockingQueueFactory = new BlockingQueueFactory();
      blockingQueueFactory.createQueue(config.getDefaultTopic());
      // Start the QueueMonitor
      queueMonitor = new QueueMonitor(this, map);
      queueMonitor.start();
      port = config.getPort();
      topicHeader = config.getTopicHeader();

      // Start the HTTP server
      Validator validator = createValidator(config.getValidatorClass());
      ChannelHandler handler = createHandler(validator);
      startServer(handler);
    } else {
      this.context().requestTaskReconfiguration();
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return WebhookSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    // Get all queue names
    List<String> queueNames = blockingQueueFactory.getAllQueues();
    int queueCount = queueNames.size();
    log.info("Queue Names - {}", queueNames);
    if (queueCount > 0) {
      int queuePerTask = Math.max(1, queueCount / maxTasks);

      Map<String, String> taskProps = new HashMap<>();
      taskProps.putAll(config.originalsStrings());
      int q = 0;
      for (int i = 0; i < maxTasks; i++) {
        List<String> taskQueues = queueNames.subList(q, q+queuePerTask);
        log.info("taskQueues {}", taskQueues);
        taskProps.put("queue.names", String.join(",", taskQueues));
        q+=queuePerTask;
        taskConfigs.add(taskProps);
      }
    }
    log.info("taskConfigs {}", taskConfigs);
    return taskConfigs;
  }

  @Override
  public void stop() {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  @Override
  public ConfigDef config() {
    return WebhookSourceConnectorConfig.config();
  }

  private Validator createValidator(String validatorClass) {
    if (validatorClass == null || validatorClass.isEmpty()) {
      return new DefaultValidator();
    } else {
      // Instantiate and configure the validator based on the provided class name
      // ...
      return (Validator) Utils.newInstance(config.getClass(validatorClass));
    }
  }
  private ChannelHandler createHandler(Validator validator) {
    return new DefaultRequestHandler(validator, blockingQueueFactory, topicHeader, config.getDefaultTopic());
  }

  private void startServer(ChannelHandler handler) {
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    try {
      log.info("Starting netty server");
      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline pipeline = ch.pipeline();
                  pipeline.addLast(new HttpRequestDecoder());
                  pipeline.addLast(new HttpResponseEncoder());
                  pipeline.addLast(handler);
                }
              })
              .option(ChannelOption.SO_BACKLOG, 128)
              .childOption(ChannelOption.SO_KEEPALIVE, true);

      bootstrap.bind("0.0.0.0", port).sync();
      log.info("Started netty server on port {}", port);
//      future.channel().closeFuture().sync();
    } catch (Exception e) {
      throw new ConnectException("Failed to start HTTP server", e);
    }
  }

//  private class RequestHandler extends SimpleChannelInboundHandler<Object> {
//    private final ObjectMapper mapper = new ObjectMapper();
//    private StringBuilder requestBodyBuilder = new StringBuilder();
//
//    private boolean readingChunks = false;
//    private HttpRequest request;
//    private String topic;
//    StringBuilder responseData = new StringBuilder();
//
//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) {
//      ctx.flush();
//    }
//
//    @Override
//    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
//      log.info("Message {}", msg);
//      if (msg instanceof HttpRequest) {
//        HttpRequest request = this.request = (HttpRequest) msg;
//        log.info("Request {}", request.decoderResult());
//
//        if (HttpUtil.is100ContinueExpected(request)) {
//          writeResponse(ctx);
//        }
//        topic = RequestUtils.topicResolver(request, topicHeader);
//        topic = topic == null ? config.getDefaultTopic() : topic;
//        responseData.setLength(0);
//        responseData.append(RequestUtils.formatParams(request));
//      }
//
//      responseData.append(RequestUtils.evaluateDecoderResult(request));
//
//      if (msg instanceof HttpContent) {
//        HttpContent httpContent = (HttpContent) msg;
//        log.info("Content {}", httpContent);
//        responseData.append(RequestUtils.formatBody(httpContent));
//        responseData.append(RequestUtils.evaluateDecoderResult(request));
//
//        ByteBuf content = httpContent.content();
//        if (content.isReadable()) {
//          requestBodyBuilder.append(content.toString(CharsetUtil.UTF_8));
//        }
//
//        if (msg instanceof LastHttpContent) {
//          String requestBody = requestBodyBuilder.toString();
//          if (readingChunks) {
//            // End of chunked encoding.
//            requestBody = requestBody.trim();
//          }
//          if (!requestBody.isEmpty()) {
//            Object jsonObject = null;
//            try {
//              jsonObject = mapper.readValue(requestBody, Object.class);
//              log.info("RequestJSON {}", jsonObject);
//              Map<String, ?> sourcePartition = new HashMap<>();
//              Map<String, ?> sourceOffset = new HashMap<>();
//              queue.add(new SourceRecord(sourcePartition, sourceOffset, topic, null, jsonObject));
//            } catch (JsonProcessingException e) {
//              log.error("Could not convert request body to JSON - ", e);
//            }
//          }
//          LastHttpContent trailer = (LastHttpContent) msg;
//          responseData.append(RequestUtils.prepareLastResponse(request, trailer));
//          writeResponse(ctx, trailer, responseData);
//        }
//      }
//    }
//
//    private void writeResponse(ChannelHandlerContext ctx) {
//      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
//      ctx.write(response);
//    }
//
//    private void writeResponse(ChannelHandlerContext ctx, LastHttpContent trailer, StringBuilder responseData) {
//      boolean keepAlive = HttpUtil.isKeepAlive(request);
//
//      FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, ((HttpObject) trailer).decoderResult()
//              .isSuccess() ? OK : BAD_REQUEST, Unpooled.copiedBuffer(responseData.toString(), CharsetUtil.UTF_8));
//
//      httpResponse.headers()
//              .set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
//
//      if (keepAlive) {
//        httpResponse.headers()
//                .setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content()
//                        .readableBytes());
//        httpResponse.headers()
//                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
//      }
//
//      ctx.write(httpResponse);
//
//      if (!keepAlive) {
//        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
//                .addListener(ChannelFutureListener.CLOSE);
//      }
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//      cause.printStackTrace();
//      ctx.close();
//    }
//  }
}
