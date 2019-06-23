package me.yuanbin.kafka.connect.protocol.constant;

/**
 * @author billryan
 * @date 2019-06-23
 */
public interface KafkaValue {
    /**
     * timestamp
     */
    String TS = "ts";

    /**
     * command type, create/insert/update/upsert/delete/remove
     */
    String COMMAND_TYPE = "command_type";

    /**
     * data
     */
    String DATA = "data";
}
