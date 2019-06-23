package me.yuanbin.kafka.connect.protocol.constant.redis;

import me.yuanbin.kafka.connect.protocol.constant.KafkaKey;

/**
 * @author billryan
 * @date 2019-06-23
 */
public interface RedisKafkaKey extends KafkaKey {
    /**
     * Single Redis Server only! Cluster does not have different databases
     */
    String DATABASE = "database";
}
