package me.yuanbin.kafka.connect.redis.sink;

import me.yuanbin.kafka.connect.redis.sink.impl.RedisSinkClientImpl;
import me.yuanbin.kafka.connect.redis.util.DataConverter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.types.Password;
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
    private RedisSinkClient client;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting RedisSinkTask.");

        RedisSinkConnectorConfig connectorConfig = new RedisSinkConnectorConfig(props);
        List<String> hosts = connectorConfig.getList(RedisSinkConnectorConfig.HOSTS_CONFIG);
        String useMode = connectorConfig.getString(RedisSinkConnectorConfig.MODE_CONFIG);
        Password redisPassword = connectorConfig.getPassword(RedisSinkConnectorConfig.PASSWORD_CONFIG);
        String password = (redisPassword == null ? null : redisPassword.value());
        int database = connectorConfig.getInt(RedisSinkConnectorConfig.DATABASE_CONFIG);
        String codec = connectorConfig.getString(RedisSinkConnectorConfig.CODEC_CONFIG);
        int connMinSize = connectorConfig.getInt(RedisSinkConnectorConfig.CONN_MIN_SIZE_CONFIG);
        DataConverter.BehaviorOnNullValues behaviorOnNullValues =
                DataConverter.BehaviorOnNullValues.forValue(
                        connectorConfig.getString(RedisSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG)
                );

        client = new RedisSinkClientImpl().Builder()
                .setHosts(hosts)
                .setUseMode(useMode)
                .setPassword(password)
                .setDatabase(database)
                .setCodec(codec)
                .setConnectionMinimumIdleSize(connMinSize)
                .setBehaviorOnNullValues(behaviorOnNullValues)
                .build();
        // TODO-是否考虑将 connector config 也放入构造器参数中?
        client.setConnectorConfig(connectorConfig);
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
