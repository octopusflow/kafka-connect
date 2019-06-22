package me.yuanbin.kafka.connect.redis;

import me.yuanbin.kafka.connect.redis.sink.RedisSinkConnectorConfig;
import me.yuanbin.kafka.connect.redis.sink.RedisSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            configProperties = props;
            // validation
            new RedisSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(
                    "Couldn't start RedisSinkConnector due to configuration error",
                    e
            );
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> config = new HashMap<>(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(config);
        }
        return taskConfigs;
    }

    @Override
    public void stop() throws ConnectException {

    }

    @Override
    public ConfigDef config() {
        return RedisSinkConnectorConfig.CONFIG_DEF;
    }
}
