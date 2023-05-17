package com.platformatory.kafka.connect;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class QueueMonitor {

    static final Logger log = LoggerFactory.getLogger(QueueMonitor.class);
    private final BlockingQueueFactory queueFactory;
    private final WebhookSourceConnector connector;

    private List<String> currentQueues;

    public QueueMonitor(WebhookSourceConnector connector) {
        this.queueFactory = WebhookSourceConnector.blockingQueueFactory;
        this.connector = connector;
        this.currentQueues = queueFactory.getAllQueues();
    }

    public void start() {
        // Schedule the monitor task to run periodically
        Timer timer = new Timer(true);
        timer.schedule(new MonitorTask(), 0, 5000); // Adjust the interval as needed
    }

    public void stop() {
        // TODO: Stop timer
    }

    private void checkQueueChanges() {
        List<String> newQueues = queueFactory.getAllQueues();

        if (!currentQueues.equals(newQueues)) {
            // Queue changes detected, trigger task reconfiguration
            currentQueues = newQueues;
            log.info("Queue changes detected, triggering task reconfiguration");
            connector.requestTaskReconfiguration();
        }
    }

    private class MonitorTask extends TimerTask {
        @Override
        public void run() {
            checkQueueChanges();
        }
    }
}
