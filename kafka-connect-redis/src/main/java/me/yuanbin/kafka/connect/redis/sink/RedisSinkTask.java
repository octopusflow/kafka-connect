package me.yuanbin.kafka.connect.redis.sink;

import me.yuanbin.kafka.connect.redis.RedisClient;
import me.yuanbin.kafka.connect.redis.impl.RedissonClientWrapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Redis Sink Task
 *
 * @author billryan
 * @date 2019-06-22
 */
public class RedisSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(RedisSinkTask.class);
    private RedisClient client;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting RedisSinkTask.");

        RedisSinkConnectorConfig connectorConfig = new RedisSinkConnectorConfig(props);
        log.info(connectorConfig.toString());
        client = new RedissonClientWrapper(connectorConfig);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        log.info("Opening the task for topic partitions: {}", partitions);
        Set<String> topics = new HashSet<>();
        for (TopicPartition tp : partitions) {
            topics.add(tp.topic());
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        log.debug("Putting {} to Redis.", records);
        client.put(records);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.trace("Flushing data to Redis with the following offsets: {}", offsets);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        log.debug("Closing the task for topic partitions: {}", partitions);
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Stopping RedisSinkTask.");
        if (client != null) {
            client.stop();
        }
    }

}
