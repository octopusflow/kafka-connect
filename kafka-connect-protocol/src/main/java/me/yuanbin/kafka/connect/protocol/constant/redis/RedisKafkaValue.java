package me.yuanbin.kafka.connect.protocol.constant.redis;

import me.yuanbin.kafka.connect.protocol.constant.KafkaValue;

/**
 * @author billryan
 * @date 2019-06-23
 */
public interface RedisKafkaValue extends KafkaValue {
    /**
     * expired at long timestamp
     */
    String EXPIRED_AT = "expired_at";

    /**
     * data type
     */
    String DATA_TYPE = "data_type";
}
