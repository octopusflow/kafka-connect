package me.yuanbin.kafka.connect.redis.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import me.yuanbin.kafka.connect.redis.RedisClient;
import me.yuanbin.kafka.connect.redis.sink.RedisSinkConnectorConfig;
import me.yuanbin.kafka.connect.protocol.util.DataConverter.BehaviorOnNullValues;
import org.apache.kafka.common.config.types.Password;
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

import static me.yuanbin.kafka.connect.protocol.constant.KafkaValue.DATA;
import static me.yuanbin.kafka.connect.protocol.constant.KafkaKey.ID;
import static me.yuanbin.kafka.connect.protocol.constant.KafkaValue.COMMAND_TYPE;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisCommandType.*;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisDataType.*;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisKafkaKey.DATABASE;
import static me.yuanbin.kafka.connect.protocol.constant.redis.RedisKafkaValue.*;
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
    private static final Codec LONG_CODEC = LongCodec.INSTANCE;
    private static final String SINGLE_SERVER = "SINGLESERVER";
    private static final long EXPIRED_15D = 15 * 24 * 3600 * 1000L;

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectReader objectReader = mapper.readerFor(new TypeReference<Object>() {});
    private static final ObjectReader listReader = mapper.readerFor(new TypeReference<List<Object>>() {});
    private static final ObjectReader setReader = mapper.readerFor(new TypeReference<Set<Object>>() {});

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
            // delete on condition behaviorOnNullValues
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
            writeRedis(redisKey, valueNode);
        }
    }

    private void writeRedis(String key, JsonNode valueNode) {
        // TODO - LONG_CODEC for collections
        String dataType = valueNode.has(DATA_TYPE) ? valueNode.get(DATA_TYPE).asText(OBJECT) : OBJECT;
        String commandType = valueNode.has(COMMAND_TYPE) ? valueNode.get(COMMAND_TYPE).asText(UNDEFINED) : UNDEFINED;
        JsonNode data = valueNode.get(DATA);
        long expiredAt = valueNode.has(EXPIRED_AT) ?
                valueNode.get(EXPIRED_AT).asLong() : System.currentTimeMillis() + EXPIRED_15D;
        boolean writable = false;
        switch (dataType) {
            case STRING:
                client.getBucket(key).set(data);
                writable = true;
                break;
            case LIST:
                writable = writeList(key, data, commandType);
                break;
            case SET:
                writable = writeSet(key, data, commandType);
                break;
            case ZSET:
                writable = writeZset(key, data, commandType);
                break;
            default:
                break;
        }

        if (writable && expiredAt > 0) { client.getBucket(key).expireAt(expiredAt); }
    }

    private boolean writeList(String key, JsonNode data, String commandType) {
        boolean writable = false;
        RList<Object> rList = client.getList(key);
        final Object listObject;
        try {
            listObject = objectReader.readValue(data);
            if (listObject instanceof Long) {
                rList = client.getList(key, LONG_CODEC);
            }
        } catch (IOException ex) {
            log.error(ex.getMessage());
            log.error(data.toString());
            return writable;
        }

        switch (commandType) {
            case LIST_ADD:
                rList.add(data);
                writable = true;
                break;
            case LIST_REM:
                rList.remove(data);
                writable = true;
                break;
            case UPSERT:
                try {
                    List<Object> dataList = listReader.readValue(data);
                    rList.clear();
                    rList.addAll(dataList);
                    writable = true;
                } catch (IOException ex) {
                    log.error(ex.getMessage());
                    log.error(data.toString());
                }
                break;
            default:
                break;
        }
        return writable;
    }

    private boolean writeSet(String key, JsonNode data, String commandType) {
        boolean writable = false;
        RSet<Object> rSet = client.getSet(key);
        final Object setObject;
        try {
            setObject = objectReader.readValue(data);
            if (setObject instanceof Long) {
                rSet = client.getSet(key, LONG_CODEC);
            }
        } catch (IOException ex) {
            log.error(ex.getMessage());
            log.error(data.toString());
            return writable;
        }

        switch (commandType) {
            case SET_ADD:
                rSet.add(setObject);
                writable = true;
                break;
            case SET_REM:
                rSet.remove(setObject);
                writable = true;
                break;
            case UPSERT:
                try {
                    Set<Object> dataList = setReader.readValue(data);
                    rSet.clear();
                    rSet.addAll(dataList);
                    writable = true;
                } catch (IOException ex) {
                    log.error(ex.getMessage());
                    log.error(data.toString());
                }
                break;
            default:
                break;
        }

        return writable;
    }

    private boolean writeZset(String key, JsonNode data, String commandType) {
        Double score = data.has(DATA_SCORE) ? data.get(DATA_SCORE).asDouble() : null;
        JsonNode zsetObjectNode = data.has(DATA_OBJECT) ? data.get(DATA_OBJECT) : null;
        boolean writable = false;
        if (score == null || zsetObjectNode == null) { return writable; }
        RScoredSortedSet<Object> rZset = client.getScoredSortedSet(key);
        final Object zsetObject;
        try {
            zsetObject = objectReader.readValue(zsetObjectNode);
            if (zsetObject instanceof Long) {
                rZset = client.getScoredSortedSet(key, LONG_CODEC);
            }
        } catch (IOException ex) {
            log.error(ex.getMessage());
            log.error(data.toString());
            return writable;
        }

        switch (commandType) {
            case ZSET_ADD:
                rZset.add(score, zsetObject);
                writable = true;
                break;
            case ZSET_INC:
                rZset.addScore(zsetObject, score);
                writable = true;
                break;
            case ZSET_REM:
                rZset.remove(zsetObject);
                writable = true;
                break;
            default:
                break;
        }
        return writable;
    }

    @Override
    public void stop() {
        log.info("shutdown redisson client...");
        client.shutdown();
    }
}
