package me.yuanbin.kafka.connect.protocol.constant;

/**
 * @author billryan
 * @date 2019-06-23
 */
public interface CommandType {
    /**
     * create
     */
    String CREATE = "create";
    String INSERT = "insert";
    String UPDATE = "update";
    String UPSERT = "upsert";
    String REMOVE = "remove";
    String DELETE = "delete";
    String UNDEFINED = "undefined";
}
