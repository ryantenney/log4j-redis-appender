# log4j-redis-appender [![Build Status](https://travis-ci.org/ryantenney/log4j-redis-appender.png?branch=master)](https://travis-ci.org/ryantenney/log4j-redis-appender)

Log4j appender for pushing log messages to a Redis list.

Derived from [@pavlobaron's log4j2redis](https://github.com/pavlobaron/log4j2redis), though the two projects share almost no code. That project writes messages to unique keys as opposed to pushing to a list.

## Configuration

This appender pushes log messages to a Redis list. Here is an example configuration:

    log4j.rootLogger=DEBUG, redis
    log4j.appender.redis=com.ryantenney.log4j.RedisAppender
    log4j.appender.redis.layout=…
    log4j.appender.redis.host=localhost
    log4j.appender.redis.port=6379
    log4j.appender.redis.password=password
    log4j.appender.redis.key=key
    log4j.appender.redis.period=500
    log4j.appender.redis.batchSize=100
    log4j.appender.redis.purgeOnFailure=true
    log4j.appender.redis.alwaysBatch=true
    log4j.appender.redis.daemonThread=true
    log4j.appender.redis.maxEvents=10000

Where:

* **key** (_required_) key of the list to push log messages
* **host** (optional, default: localhost)
* **port** (optional, default: 6379)
* **password** (optional) redis password, if required
* **period** (optional, default: 500) the period in milliseconds between 
* **batchSize** (optional, default: 100) the number of log messages to send in a single `RPUSH` command
* **purgeOnFailure** (optional, default: true) whether to purge the enqueued log messages if an error occurs attempting to connect to redis, thus preventing the memory usage from becoming too high
* **alwaysBatch** (optional, default: true) whether to wait for a full batch. if true, will only send once there are `batchSize` log messages enqueued
* **daemonThread** (optional, default: true) whether to launch the appender thread as a daemon thread
* **maxEvents** (optional, default: Integer.MAX\_VALUE) the maximum number of log4j events to accept when Redis is unresponsive

### Maven

```xml
<dependency>
	<groupId>com.ryantenney.log4j</groupId>
	<artifactId>redis-appender</artifactId>
	<version>1.0.0</version>
</dependency>
```

### Usage Note

Goes great with [@lusis's log4j-jsonevent-layout](https://github.com/lusis/log4j-jsonevent-layout) for pushing log messages straight to a [Logstash](https://github.com/logstash/logstash) instance configured to ingest log messages from Redis. If you use Logstash with AMQP, check out [@lusis's ZeroMQ Appender](https://github.com/lusis/zmq-appender) or [@jbrisbin's RabbitMQ Appender](https://github.com/jbrisbin/vcloud/tree/master/amqp-appender)

### License

Copyright (c) 2012-2013 Ryan Tenney

Published under Apache Software License 2.0, see LICENSE

[![Rochester Made](http://rochestermade.com/media/images/rochester-made-dark-on-light.png)](http://rochestermade.com)
