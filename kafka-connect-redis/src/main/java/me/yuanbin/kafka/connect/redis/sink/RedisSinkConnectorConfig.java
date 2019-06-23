package me.yuanbin.kafka.connect.redis.sink;

import me.yuanbin.kafka.connect.protocol.util.DataConverter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Redis Sink Connector Config with Redisson
 *
 * @author billryan
 * @date 2019-06-22
 */
public class RedisSinkConnectorConfig extends AbstractConfig {

    public static final String HOSTS_CONFIG = "redis.hosts";
    private static final String HOSTS_DOC = "The Redis hosts to connect to.";
    public static final String PASSWORD_CONFIG = "redis.password";
    private static final String PASSWORD_DOC = "Password used to connect to Redis.";
    public static final String DATABASE_CONFIG = "redis.database";
    private static final String DATABASE_DOC = "Redis database to connect to.";
    public static final String MODE_CONFIG = "redis.mode";
    private static final String MODE_DOC = "Redis use mode(single/cluster/master-slave/...).";
    public static final String CODEC_CONFIG = "redis.codec";
    private static final String CODEC_DOC = "Redis codec.";
    public static final String CONN_MIN_SIZE_CONFIG = "redis.connectionMinimumIdleSize";
    private static final String CONN_MIN_SIZE_DOC = "Minimum idle Redis connection amount.";
    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
            + "non-null key and a null value (i.e. Kafka tombstone records). Valid options are "
            + "'ignore', 'delete', and 'fail'.";

    private static ConfigDef getConfigDef() {
        int order = 0;
        final String group = "RedisConnector";
        return new ConfigDef()
                .define(
                        HOSTS_CONFIG,
                        ConfigDef.Type.LIST,
                        "127.0.0.1:6379",
                        ConfigDef.Importance.HIGH,
                        HOSTS_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.LONG,
                        "Redis Host URLs"
                ).define(
                        PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        null,
                        ConfigDef.Importance.LOW,
                        PASSWORD_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.SHORT,
                        "Redis Password"
                ).define(
                        DATABASE_CONFIG,
                        ConfigDef.Type.INT,
                        0,
                        ConfigDef.Importance.HIGH,
                        DATABASE_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.SHORT,
                        "Redis Database ID"
                ).define(
                        MODE_CONFIG,
                        ConfigDef.Type.STRING,
                        "singleServer",
                        ConfigDef.Importance.LOW,
                        MODE_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.SHORT,
                        "Redis Use Mode"
                ).define(
                        CODEC_CONFIG,
                        ConfigDef.Type.STRING,
                        "org.redisson.codec.JsonJacksonCodec",
                        ConfigDef.Importance.LOW,
                        CODEC_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.SHORT,
                        "Redis Codec"
                ).define(
                        CONN_MIN_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        8,
                        ConfigDef.Importance.LOW,
                        CONN_MIN_SIZE_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.SHORT,
                        "Redis Connection Size"
                ).define(
                        BEHAVIOR_ON_NULL_VALUES_CONFIG,
                        ConfigDef.Type.STRING,
                        DataConverter.BehaviorOnNullValues.IGNORE.toString(),
                        DataConverter.BehaviorOnNullValues.VALIDATOR,
                        ConfigDef.Importance.LOW,
                        BEHAVIOR_ON_NULL_VALUES_DOC,
                        group,
                        ++order,
                        ConfigDef.Width.SHORT,
                        "Behavior for null-valued records"
                );
    }

    public static final ConfigDef CONFIG_DEF = getConfigDef();

    public RedisSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

}
