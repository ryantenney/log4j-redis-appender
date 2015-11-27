package com.ryantenney.log4j;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisAppenderRunnableTest {

    private int batchSize = 10;

    private RedisAppenderRunnable runnable;

    @Mock
    private ErrorHandler errorHandler;

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    @Mock
    private Layout layout;

    @Before
    public void setUp() {
        runnable = new RedisAppenderRunnable();
        runnable.setAlwaysBatch(false);
        runnable.setBatchSize(batchSize);
        runnable.setConnectionPoolRetryCount(0);
        runnable.setErrorHandler(errorHandler);
        runnable.setJedisPool(jedisPool);
        runnable.setLayout(layout);
        runnable.setMaxEvents(Integer.MAX_VALUE);
        runnable.setKey("key");

        when(jedisPool.getResource()).thenReturn(jedis);
    }

    @Test
    public void batchIsPushedToRedis() {
        for (int i = 0; i < 10; i++) {
            runnable.add(loggingEvent(String.valueOf(i)));
        }
        runnable.run();

        // capturing varargs unfortunately isn't possible with Mockito currently
        // the messages are just encoded in their ASCII value
        verify(jedis).rpush("key".getBytes(),
                new byte[]{48}, new byte[]{49},
                new byte[]{50}, new byte[]{51},
                new byte[]{52}, new byte[]{53},
                new byte[]{54}, new byte[]{55},
                new byte[]{56}, new byte[]{57}
        );
    }

    @Test
    public void exceptionDuringPush() {
        // sending the first ten will result in an Exception
        // the additional message should be appended to the end
        when(jedis.rpush("key".getBytes(),
                new byte[]{48}, new byte[]{49},
                new byte[]{50}, new byte[]{51},
                new byte[]{52}, new byte[]{53},
                new byte[]{54}, new byte[]{55},
                new byte[]{56}, new byte[]{57}
        )).thenThrow(new JedisDataException(""));

        for (int i = 0; i < 10; i++) {
            runnable.add(loggingEvent(String.valueOf(i)));
        }
        runnable.add(loggingEvent("foo"));

        runnable.run();

        verify(jedis).rpush("key".getBytes(),
                new byte[]{48}, new byte[]{49},
                new byte[]{50}, new byte[]{51},
                new byte[]{52}, new byte[]{53},
                new byte[]{54}, new byte[]{55},
                new byte[]{56}, new byte[]{57}
        );

        // the last message should be overwritten
        verify(jedis).rpush("key".getBytes(),
                new byte[]{48}, new byte[]{49},
                new byte[]{50}, new byte[]{51},
                new byte[]{52}, new byte[]{53},
                new byte[]{54}, new byte[]{55},
                new byte[]{56}, "foo".getBytes()
        );
    }

    @Test
    public void maxEventsCanBeConfigured() {
        runnable.setMaxEvents(10);
        for (int i = 0; i < 11; i++) {
            runnable.add(loggingEvent(String.valueOf(i)));
        }

        assertEquals(10, runnable.eventsSize());
    }

    private LoggingEvent loggingEvent(String text) {
        LoggingEvent event = mock(LoggingEvent.class);
        when(layout.format(event)).thenReturn(text);
        return event;
    }

}
