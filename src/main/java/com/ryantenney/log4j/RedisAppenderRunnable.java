package com.ryantenney.log4j;

import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LoggingEvent;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.SafeEncoder;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Florian Hopf. mail@florian-hopf.de
 */
public class RedisAppenderRunnable implements Runnable {


    private int batchSize;
    private int messageIndex = 0;

    private Queue<LoggingEvent> events = new ConcurrentLinkedQueue<LoggingEvent>();
    private Layout layout;
    private byte[][] batch = new byte[batchSize][];
    private ErrorHandler errorHandler;
    private boolean alwaysBatch;
    private JedisPool jedisPool;
    private int connectionPoolRetryCount;
    private boolean purgeOnFailure;

    public void run() {

        try {
            if (messageIndex == batchSize) push();

            LoggingEvent event;
            while ((event = events.poll()) != null) {
                try {
                    String message = layout.format(event);
                    batch[messageIndex++] = SafeEncoder.encode(message);
                } catch (Exception e) {
                    errorHandler.error(e.getMessage(), e, ErrorCode.GENERIC_FAILURE, event);
                }

                if (messageIndex == batchSize) push();
            }

            if (!alwaysBatch && messageIndex > 0) push();
        } catch (Exception e) {
            errorHandler.error(e.getMessage(), e, ErrorCode.WRITE_FAILURE);
        }
    }

    private void push() {
        LogLog.debug("Sending " + messageIndex + " log messages to Redis");
        Jedis connection = jedisPool.getResource();
        try {
            // this may all be unnecessary with the pooling set up right just being super cautious
            if (!connection.isConnected()) {
                jedisPool.returnBrokenResource(connection);
                for (int i = 0; i < connectionPoolRetryCount; i++) {
                    connection = jedisPool.getResource();
                    if (connection.isConnected()) {
                        break;
                    } else {
                        if (i >= connectionPoolRetryCount) {
                            // something wrong
                            if (purgeOnFailure) {
                                LogLog.debug("Purging Redis event queue");
                                events.clear();
                                messageIndex = 0;
                            }
                            jedisPool.returnBrokenResource(connection);
                            LogLog.error("Error during getting connection from pool check your Redis settings");
                            return;
                        }
                    }

                }

            }
            connection.rpush(SafeEncoder.encode(key),
                    batchSize == messageIndex
                            ? batch
                            : Arrays.copyOf(batch, messageIndex));
            messageIndex = 0;
        } catch (Exception e) {
            LogLog.error("Error pushing message list to Redis",e);
        } finally {
            if (connection!=null) {
                jedisPool.returnResource(connection);
            }
        }
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setLayout(Layout layout) {
        this.layout = layout;
    }

    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public void setAlwaysBatch(boolean alwaysBatch) {
        this.alwaysBatch = alwaysBatch;
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void setConnectionPoolRetryCount(int connectionPoolRetryCount) {
        this.connectionPoolRetryCount = connectionPoolRetryCount;
    }

    public void setPurgeOnFailure(boolean purgeOnFailure) {
        this.purgeOnFailure = purgeOnFailure;
    }

    public void add(LoggingEvent event) {
        events.add(event);
    }
}
