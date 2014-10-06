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
* @copyright 2012 Pavlo Baron
**/

package com.ryantenney.log4j;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.SafeEncoder;

public class RedisAppender extends AppenderSkeleton implements Runnable {

	private String host = "localhost";
	private int port = 6379;
	private String password;
	private String key;

	private int batchSize = 100;
	private long period = 500;
	private boolean alwaysBatch = true;
	private boolean purgeOnFailure = true;
	private boolean daemonThread = true;

	private int messageIndex = 0;
	private Queue<LoggingEvent> events;
	private byte[][] batch;

	private Jedis jedis;

	private ScheduledExecutorService executor;
	private ScheduledFuture<?> task;

	@Override
	public void activateOptions() {
		try {
			super.activateOptions();

			if (key == null) throw new IllegalStateException("Must set 'key'");

			if (executor == null) executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("RedisAppender", daemonThread));

			if (task != null && !task.isDone()) task.cancel(true);
			if (jedis != null && jedis.isConnected()) jedis.disconnect();

			events = new ConcurrentLinkedQueue<LoggingEvent>();
			batch = new byte[batchSize][];
			messageIndex = 0;

			jedis = new Jedis(host, port);
			task = executor.scheduleWithFixedDelay(this, period, period, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			LogLog.error("Error during activateOptions", e);
		}
	}

	@Override
	protected void append(LoggingEvent event) {
		try {
			populateEvent(event);
			events.add(event);
		} catch (Exception e) {
			errorHandler.error("Error populating event and adding to queue", e, ErrorCode.GENERIC_FAILURE, event);
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
			jedis.disconnect();
		} catch (Exception e) {
			errorHandler.error(e.getMessage(), e, ErrorCode.CLOSE_FAILURE);
		}
	}

	private boolean connect() {
		try {
			if (!jedis.isConnected()) {
				LogLog.debug("Connecting to Redis: " + host);
				jedis.connect();

				if (password != null) {
					String result = jedis.auth(password);
					if (!"OK".equals(result)) {
						LogLog.error("Error authenticating with Redis: " + host);
					}
				}
			}
			return true;
		} catch (Exception e) {
			LogLog.error("Error connecting to Redis server", e);
			return false;
		}
	}

	public void run() {
		if (!connect()) {
			if (purgeOnFailure) {
				LogLog.debug("Purging event queue");
				events.clear();
				messageIndex = 0;
			}
			return;
		}

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
		try {
			jedis.rpush(SafeEncoder.encode(key),
				batchSize == messageIndex
					? batch
					: Arrays.copyOf(batch, messageIndex));
			messageIndex = 0;
		} catch (JedisConnectionException e) {
			LogLog.warn("Lost connection to Redis: " + e.getMessage());
			jedis.disconnect();
			if (connect()) {
				push();
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			events.clear();
			messageIndex = 0;
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

	public boolean requiresLayout() {
		return true;
	}

}
