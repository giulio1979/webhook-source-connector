package com.platformatory.kafka.connect;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockingQueueFactory {

    private Map<String, BlockingQueue<SourceRecord>> queueRegistry;

    public BlockingQueueFactory() {
        queueRegistry = new ConcurrentHashMap<>();
    }

    public BlockingQueue<SourceRecord> createQueue(String queueName) {
        BlockingQueue<SourceRecord> queue = new LinkedBlockingQueue<>();
        queueRegistry.put(queueName, queue);
        return queue;
    }

    public BlockingQueue<SourceRecord> getQueue(String queueName) {
        return queueRegistry.get(queueName);
    }

    public BlockingQueue<SourceRecord> getOrCreateQueue(String queueName) {
        if(queueRegistry.containsKey(queueName)) {
            return queueRegistry.get(queueName);
        }
        BlockingQueue<SourceRecord> queue = new LinkedBlockingQueue<>();
        queueRegistry.put(queueName, queue);
        return queue;
    }

    public void removeQueue(String queueName) {
        queueRegistry.remove(queueName);
    }

    public void clearAllQueues() {
        queueRegistry.clear();
    }

    public List<String> getAllQueues() {
        return new ArrayList<>(queueRegistry.keySet());
    }


}
