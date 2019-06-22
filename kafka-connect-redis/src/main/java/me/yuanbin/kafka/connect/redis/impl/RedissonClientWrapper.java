package me.yuanbin.kafka.connect.redis.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.yuanbin.kafka.connect.redis.RedisClient;
import me.yuanbin.kafka.connect.redis.sink.RedisSinkConnectorConfig;
import me.yuanbin.kafka.connect.redis.util.DataConverter.BehaviorOnNullValues;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.redisson.api.RType.LIST;
import static org.redisson.api.RType.SET;
import static org.redisson.api.RType.ZSET;
import static org.redisson.api.RType.MAP;

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
            if (record.value() == null) continue;
            log.debug(
                    "record keySchema {}, key {}, valueSchema {}, value {}",
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    record.value());

            String rawKey = convertKey(record);
            String key = rawKey;
            long expiredAt = -1;
            JsonNode rootValue = convertValue(record);

        }
    }

    private String convertKey(SinkRecord record) {
        if (record.key() == null) return null;
        String key = null;
        Schema keySchema = record.keySchema();
        if (keySchema != null) {
            switch (keySchema.type()) {
                case STRING:
                    key = (String) record.key();
                    break;
                case BYTES:
                    key = new String((byte[])record.key());
                    break;
                default:
                    key = record.key().toString();
            }
        } else {
            key = record.key().toString();
        }

        return key;
    }

    private JsonNode convertValue(SinkRecord record) {
        JsonNode rootValue = null;
        try {
            Schema valueSchema = record.valueSchema();
            if (valueSchema != null) {
                switch (valueSchema.type()) {
                    case STRING:
                        rootValue = mapper.readTree((String) record.value());
                        break;
                    case BYTES:
                        rootValue = mapper.readTree((byte[]) record.value());
                        break;
                    default:
                        rootValue = mapper.readTree(record.value().toString());
                }
            } else {
                rootValue = mapper.readTree(record.value().toString());
            }
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }

        return rootValue;
    }


    @Override
    public void stop() {
        log.info("shutdown redisson client...");
        client.shutdown();
    }
}
