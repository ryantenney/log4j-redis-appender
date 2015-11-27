/**
* This file is part of log4j2redis
*
* Copyright (c) 2012 by Pavlo Baron (pb at pbit dot org)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*

* @author Pavlo Baron <pb at pbit dot org>
* @author Landro Silva
* @author Ryan Tenney <ryan@10e.us>
* @author Ryan Vanderwerf <rvanderwerf @ gmail dot com>
* @copyright 2012 Pavlo Baron
**/

package com.ryantenney.log4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisAppender extends AppenderSkeleton {

	private String host = "localhost";
	private int port = 6379;
	private String password;
	private String key;

    // these are the jedispoolconfig object settings
    private int timeout = 2000;
	private int batchSize = 100;
	private long period = 500;
	private boolean alwaysBatch = true;
	private boolean purgeOnFailure = true;
	private boolean daemonThread = true;
    private long minEvictableIdleTimeMillis = 60000L;
    private long timeBetweenEvictionRunsMillis = 30000L;
    private int numTestsPerEvictionRun = -1;
    private int maxTotal = 8;
    private int maxIdle = 0;
    private int minIdle = 0;
    private boolean blockWhenExhaused = false;
    private String evictionPolicyClassName = "";
    private boolean lifo = false;
    private boolean testOnBorrow = false;
    private boolean testWhileIdle = false;
    private boolean testOnReturn = false;
    private int connectionPoolRetryCount = 2;
    private int maxEvents = Integer.MAX_VALUE;

    private JedisPool jedisPool;

	private ScheduledExecutorService executor;
	private ScheduledFuture<?> task;


    private RedisAppenderRunnable redisAppenderRunnable;

	@Override
	public void activateOptions() {
		try {
			super.activateOptions();

			if (key == null) throw new IllegalStateException("Must set 'key'");

			if (executor == null) executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("RedisAppender", daemonThread));

			if (task != null && !task.isDone()) task.cancel(true);

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            if (lifo) {
                poolConfig.setLifo(lifo);
            }
            if (testOnBorrow) {
                poolConfig.setTestOnBorrow(testOnBorrow);
            }
            if (isTestWhileIdle()) {
                poolConfig.setTestWhileIdle(isTestWhileIdle());
            }
            if (testOnReturn) {
                poolConfig.setTestOnReturn(testOnReturn);
            }
            if (timeBetweenEvictionRunsMillis > 0) {
                poolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
            }
            if (evictionPolicyClassName!=null && evictionPolicyClassName.length() >0) {
                poolConfig.setEvictionPolicyClassName(evictionPolicyClassName);
            }
            if (blockWhenExhaused) {
                poolConfig.setBlockWhenExhausted(blockWhenExhaused);
            }
            if (minIdle > 0) {
                poolConfig.setMinIdle(minIdle);
            }
            if (maxIdle > 0) {
                poolConfig.setMaxIdle(maxIdle);
            }
            if (numTestsPerEvictionRun > 0) {
                poolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
            }
            if (maxTotal != 8) {
                poolConfig.setMaxTotal(maxTotal);
            }
            if (minEvictableIdleTimeMillis > 0) {
                poolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
            }

            if (password!=null && password.length()>0) {
                jedisPool = new JedisPool(poolConfig,host,port,timeout,password);
            } else {
                jedisPool = new JedisPool(poolConfig,host,port,timeout);
            }

            redisAppenderRunnable = new RedisAppenderRunnable();
            redisAppenderRunnable.setBatchSize(batchSize);
            redisAppenderRunnable.setLayout(layout);
            redisAppenderRunnable.setErrorHandler(errorHandler);
            redisAppenderRunnable.setAlwaysBatch(alwaysBatch);
            redisAppenderRunnable.setJedisPool(jedisPool);
            redisAppenderRunnable.setConnectionPoolRetryCount(connectionPoolRetryCount);
            redisAppenderRunnable.setPurgeOnFailure(purgeOnFailure);
            redisAppenderRunnable.setKey(key);
            redisAppenderRunnable.setMaxEvents(maxEvents);

            task = executor.scheduleWithFixedDelay(redisAppenderRunnable, period, period, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			LogLog.error("RedisAppender: Error during activateOptions", e);
		}
	}

	@Override
	protected void append(LoggingEvent event) {
		try {
			populateEvent(event);
            redisAppenderRunnable.add(event);
		} catch (Exception e) {
			errorHandler.error("Error populating event and adding Redis to queue", e, ErrorCode.GENERIC_FAILURE, event);
		}
	}

	protected void populateEvent(LoggingEvent event) {
		event.getThreadName();
		event.getRenderedMessage();
		event.getNDC();
		event.getMDCCopy();
		event.getThrowableStrRep();
		event.getLocationInformation();
	}

	@Override
	public void close() {
		try {
			task.cancel(false);
			executor.shutdown();
		} catch (Exception e) {
			errorHandler.error(e.getMessage(), e, ErrorCode.CLOSE_FAILURE);
		}
	}


	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setPeriod(long millis) {
		this.period = millis;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setBatchSize(int batchsize) {
		this.batchSize = batchsize;
	}

	public void setPurgeOnFailure(boolean purgeOnFailure) {
		this.purgeOnFailure = purgeOnFailure;
	}

	public void setAlwaysBatch(boolean alwaysBatch) {
		this.alwaysBatch = alwaysBatch;
	}

	public void setDaemonThread(boolean daemonThread){
		this.daemonThread = daemonThread;
	}

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public long getMinEvictableIdleTimeMillis() {
        return minEvictableIdleTimeMillis;
    }

    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public long getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public boolean isBlockWhenExhaused() {
        return blockWhenExhaused;
    }

    public void setBlockWhenExhaused(boolean blockWhenExhaused) {
        this.blockWhenExhaused = blockWhenExhaused;
    }

    public String getEvictionPolicyClassName() {
        return evictionPolicyClassName;
    }

    public void setEvictionPolicyClassName(String evictionPolicyClassName) {
        this.evictionPolicyClassName = evictionPolicyClassName;
    }

    public boolean isLifo() {
        return lifo;
    }

    public void setLifo(boolean lifo) {
        this.lifo = lifo;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    public void setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public boolean requiresLayout() {
		return true;
	}

    public int getConnectionPoolRetryCount() {
        return connectionPoolRetryCount;
    }

    public void setConnectionPoolRetryCount(int connectionPoolRetryCount) {
        this.connectionPoolRetryCount = connectionPoolRetryCount;
    }

    public void setMaxEvents(int maxEvents) {
        this.maxEvents = maxEvents;
    }

    public int getMaxEvents() {
        return maxEvents;
    }
}
