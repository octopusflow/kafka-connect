package me.yuanbin.kafka.connect.protocol.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

import static me.yuanbin.kafka.connect.protocol.constant.KafkaKey.ID;
import static me.yuanbin.kafka.connect.protocol.constant.KafkaValue.DATA;

/**
 * @author billryan
 * @date 2019-06-22
 */
public class DataConverter {

    private static final Logger log = LoggerFactory.getLogger(DataConverter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String JSON_OBJECT_START_STR = "{";

    /**
     * behavior on null values
     */
    public enum BehaviorOnNullValues {
        /**
         * ignore
         */
        IGNORE,
        /**
         * delete
         */
        DELETE,
        /**
         * fail
         */
        FAIL;

        public static final BehaviorOnNullValues DEFAULT = IGNORE;

        // Want values for "behavior.on.null.values" property to be case-insensitive
        public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
            private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

            @Override
            public void ensureValid(String name, Object value) {
                if (value instanceof String) {
                    value = ((String) value).toLowerCase(Locale.ROOT);
                }
                validator.ensureValid(name, value);
            }

            // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
            @Override
            public String toString() {
                return validator.toString();
            }

        };

        public static String[] names() {
            BehaviorOnNullValues[] behaviors = values();
            String[] result = new String[behaviors.length];

            for (int i = 0; i < behaviors.length; i++) {
                result[i] = behaviors[i].toString();
            }

            return result;
        }

        public static BehaviorOnNullValues forValue(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static JsonNode convertKey(SinkRecord record) {
        return convert(record.key(), record.keySchema(), ID);
    }

    public static JsonNode convertValue(SinkRecord record) {
        return convert(record.value(), record.valueSchema(), DATA);
    }

    private static JsonNode convert(Object object, Schema schema, String fallbackKey) {
        if (object == null) {
            throw new ConnectException("object can not be null.");
        }

        final Schema.Type schemaType;
        if (schema == null) {
            schemaType = ConnectSchema.schemaType(object.getClass());
            if (schemaType == null) {
                throw new DataException(
                        "Java class "
                                + object.getClass()
                                + " does not have corresponding schema type."
                );
            }
        } else {
            schemaType = schema.type();
        }

        final String rawData;
        switch (schemaType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case BYTES:
            case STRING:
                rawData = String.valueOf(object);
                break;
            default:
                throw new DataException(schemaType.name() + " is not supported as JsonNode.");
        }

        return parse(rawData, fallbackKey);
    }

    private static JsonNode parse(String raw, String fallbackKey) {
        JsonNode rootValue = null;
        if (raw.startsWith(JSON_OBJECT_START_STR)) {
            try {
                rootValue = OBJECT_MAPPER.readTree(raw);
            } catch (IOException ex) {
                log.warn(ex.getMessage());
            }
        } else {
            rootValue = OBJECT_MAPPER.createObjectNode().put(fallbackKey, raw);
        }
        // validate fallbackKey
        if (rootValue != null && !rootValue.hasNonNull(fallbackKey)) {
            log.warn("object {} does not have fallbackKey: {}", raw, fallbackKey);
            rootValue = null;
        }
        return rootValue;
    }
}
