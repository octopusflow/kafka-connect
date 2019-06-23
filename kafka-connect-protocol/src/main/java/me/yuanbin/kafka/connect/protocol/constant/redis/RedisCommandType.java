package me.yuanbin.kafka.connect.protocol.constant.redis;

import me.yuanbin.kafka.connect.protocol.constant.CommandType;

/**
 * @author billryan
 * @date 2019-06-23
 */
public interface RedisCommandType extends CommandType {
    /**
     * zset inc, add, rem
     */
    String SET_ADD = "set_add";
    String SET_REM = "set_rem";
    String ZSET_INC = "zset_inc";
    String ZSET_ADD = "zset_add";
    String ZSET_REM = "zset_rem";
    String LIST_ADD = "list_add";
    String LIST_REM = "list_rem";
    String MAP_PUT = "map_put";
    String MAP_REM = "map_rem";
}
