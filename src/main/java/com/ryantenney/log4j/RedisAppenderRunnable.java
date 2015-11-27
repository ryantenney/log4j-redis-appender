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
 * Keeps the batch of messages in memory and pushes them to Redis.
 */
public class RedisAppenderRunnable implements Runnable {


    private int batchSize;
    private int messageIndex = 0;

    private Queue<LoggingEvent> events = new ConcurrentLinkedQueue<LoggingEvent>();
    private Layout layout;
    private byte[][] batch;
    private ErrorHandler errorHandler;
    private boolean alwaysBatch;
    private JedisPool jedisPool;
    private int connectionPoolRetryCount;
    private boolean purgeOnFailure;
    private String key;
    private int maxEvents;

    private String lastExceptionMessage;

    public void run() {

        try {
            if (messageIndex == batchSize) push();

            LoggingEvent event;
            while ((event = events.poll()) != null) {
                try {
                    String message = layout.format(event);
                    // if there has been an error the messageIndex might be too large
                    // reset it to a valid value, overwrite the last message in the batch
                    if (messageIndex >= batchSize) {
                        messageIndex = batchSize - 1;
                    }
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
            lastExceptionMessage = null;
        } catch (Exception e) {
            // In case of an OOM this Exception might be triggered a lot. Only log it once completely.
            if (e.getMessage() != null && e.getMessage().equals(lastExceptionMessage)) {
                LogLog.error("Error pushing message list to Redis: " + e.getMessage());
            } else {
                LogLog.error("Error pushing message list to Redis",e);
            }
            lastExceptionMessage = e.getMessage();
        } finally {
            if (connection!=null) {
                jedisPool.returnResource(connection);
            }
        }
    }

    public void setBatchSize(int batchSize) {
        batch = new byte[batchSize][];
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
        if (events.size() < maxEvents) {
            events.add(event);
        }
    }

    long eventsSize() {
        return events.size();
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setMaxEvents(int maxEvents) {
        this.maxEvents = maxEvents;
    }
}
