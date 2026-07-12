package coinsight.arbitrage.shared.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class ProtobufDeserializer<T extends com.google.protobuf.Message> implements Deserializer<T> {

    private final Parser<T> parser;

    public ProtobufDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed for basic protobuf deserialization
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to deserialize protobuf message from topic: {}", topic, e);
            throw new RuntimeException("Failed to deserialize protobuf message", e);
        }
    }

    @Override
    public void close() {
        // No resources to clean up
    }
}