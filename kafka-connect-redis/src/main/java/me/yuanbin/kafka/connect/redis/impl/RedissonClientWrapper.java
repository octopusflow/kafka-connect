package me.yuanbin.kafka.connect.redis.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.yuanbin.kafka.connect.redis.RedisClient;
import me.yuanbin.kafka.connect.redis.sink.RedisSinkConnectorConfig;
import me.yuanbin.kafka.connect.protocol.util.DataConverter.BehaviorOnNullValues;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.sink.SinkRecord;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.Codec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisCommandType.UNDEFINED;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisCommandType.DELETE;
import static me.yuanbin.kafka.connect.protocol.constant.KafkaKey.ID;
import static me.yuanbin.kafka.connect.protocol.constant.KafkaValue.COMMAND_TYPE;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisDataType.OBJECT;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisKafkaKey.DATABASE;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisKafkaValue.DATA_TYPE;
import static me.yuanbin.kafka.connect.protocol.util.DataConverter.convertKey;
import static me.yuanbin.kafka.connect.protocol.util.DataConverter.convertValue;

/**
 * @author billryan
 * @date 2019-06-22
 */
public class RedissonClientWrapper implements RedisClient {

    private static final Logger log = LoggerFactory.getLogger(RedissonClientWrapper.class);

    private final BehaviorOnNullValues behaviorOnNullValues;
    private final RedissonClient client;
    private static final String SINGLE_SERVER = "SINGLESERVER";

    private ObjectMapper mapper = new ObjectMapper();

    public RedissonClientWrapper(RedisSinkConnectorConfig connectorConfig) {
        List<String> hosts = connectorConfig.getList(RedisSinkConnectorConfig.HOSTS_CONFIG);
        String useMode = connectorConfig.getString(RedisSinkConnectorConfig.MODE_CONFIG);
        Password redisPassword = connectorConfig.getPassword(RedisSinkConnectorConfig.PASSWORD_CONFIG);
        String password = redisPassword == null ? null : redisPassword.value();
        int database = connectorConfig.getInt(RedisSinkConnectorConfig.DATABASE_CONFIG);
        String codec = connectorConfig.getString(RedisSinkConnectorConfig.CODEC_CONFIG);
        int connMinSize = connectorConfig.getInt(RedisSinkConnectorConfig.CONN_MIN_SIZE_CONFIG);
        this.behaviorOnNullValues =
                BehaviorOnNullValues.forValue(
                        connectorConfig.getString(RedisSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG)
                );

        Config config = new Config();
        if (null == useMode) {
            useMode = "singleServer";
        }
        // single_server ==> singleserver
        useMode = useMode.replace("_", "").toUpperCase();
        switch (useMode) {
            case SINGLE_SERVER:
                SingleServerConfig serverConfig = config.useSingleServer();
                serverConfig.setAddress(String.format("redis://%s", hosts.get(0)));
                serverConfig.setPassword(password);
                serverConfig.setDatabase(database);
                serverConfig.setConnectionMinimumIdleSize(connMinSize);
                break;
            default:
                break;
        }

        try {
            Codec newCodec = (Codec) Class.forName(codec).newInstance();
            config.setCodec(newCodec);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            log.error(ex.getMessage());
        }

        // debug
        try {
            String configJson = config.toJSON();
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }

        client = Redisson.create(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            if (record == null) {
                log.error("record is null!!!");
                continue;
            }
            log.debug(
                    "record keySchema {}, key {}, valueSchema {}, value {}",
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    record.value());

            JsonNode keyNode = convertKey(record);

            String redisKey = keyNode == null ? null : keyNode.get(ID).asText();
            if (redisKey == null || redisKey.isEmpty()) {
                log.error("redis key is null!!!");
                log.error(record.toString());
                continue;
            }
            int database = keyNode.get(DATABASE).asInt();

            JsonNode valueNode = convertValue(record);
            final String commandType;
            if (valueNode == null) {
                switch (behaviorOnNullValues) {
                    case IGNORE:
                        continue;
                    case DELETE:
                        commandType = DELETE;
                        break;
                    case FAIL:
                        // TODO
                    default:
                        continue;
                }
            } else {
                commandType = valueNode.has(COMMAND_TYPE) ? valueNode.get(COMMAND_TYPE).asText(UNDEFINED) : UNDEFINED;
            }
            if (commandType.equals(DELETE)) {
                client.getKeys().delete(redisKey);
                continue;
            }
            String dataType = valueNode.has(DATA_TYPE) ? valueNode.get(DATA_TYPE).asText(OBJECT) : OBJECT;
        }
    }

    @Override
    public void stop() {
        log.info("shutdown redisson client...");
        client.shutdown();
    }
}
