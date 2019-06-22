package me.yuanbin.kafka.connect.redis.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * @author billryan
 * @date 2019-06-22
 */
public interface RedisSinkClient {

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

    /**
     * set connector config
     *
     * @param connectorConfig connector config
     */
    void setConnectorConfig(AbstractConfig connectorConfig);
}
