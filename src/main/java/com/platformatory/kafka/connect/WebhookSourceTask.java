package com.platformatory.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class WebhookSourceTask extends SourceTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  static final Logger log = LoggerFactory.getLogger(WebhookSourceTask.class);

  private WebhookSourceConnectorConfig config;

  private long pollInterval;

  private Long pollTimestamp;

  private List<BlockingQueue<SourceRecord>> queues;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> map) {

    log.info("Task config map {}", map);
    config = new WebhookSourceConnectorConfig(map);
    pollInterval = config.getPollInterval();
    String queueNames = map.get("queue.names");
    queues = new ArrayList<>(queueNames.split(",").length);
    log.info("Queue.names {}", queueNames);
    for(String queue : queueNames.split(",")) {
      queues.add(WebhookSourceConnector.blockingQueueFactory.getQueue(queue));
    }
    log.info("task started for queues - {}",queueNames);
  }

  @Override
  public List<SourceRecord> poll() {
    Long currentTimestamp = System.currentTimeMillis();
    if (pollTimestamp == null) {
      pollTimestamp = currentTimestamp;
    }
    if(currentTimestamp - pollTimestamp >= pollInterval || currentTimestamp == pollTimestamp) {
      pollTimestamp = currentTimestamp;
      List<SourceRecord> records = new ArrayList<>();
      for (BlockingQueue<SourceRecord> queue : queues) {
        int queueSize = queue.size();
        log.info("Queue size: {}. ", queueSize);
        List<SourceRecord> queueRecords = new ArrayList<>(queueSize);
        queue.drainTo(queueRecords, queueSize);
        queue.clear();
        records.addAll(queueRecords);
      }
      return records;
    } else {
//      log.trace("Connector task: Pausing for poll interval");
      return new ArrayList<>();
    }
  }

  @Override
  public void stop() {
  }


}