package me.yuanbin.kafka.connect.redis;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * @author billryan
 * @date 2019-06-22
 */
public interface RedisClient {

    /**
     * put SinkRecord
     *
     * @param records Kafka Sink Records
     */
    void put(Collection<SinkRecord> records);

    /**
     * shut down the client.
     */
    void stop();
}
