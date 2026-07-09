package coinsight.arbitrage.shared.util;

import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * Serializes a protobuf message to its canonical JSON representation instead of the
 * compact binary wire format.
 *
 * <p>Used only for monitor-topic/monitor-dlt-topic, not the exchange ticker topics.
 * Those stay binary ({@link ProtobufSerializer}) since throughput/size genuinely matter
 * there. Monitoring events are low volume and exist to be read (by a human in Grafana,
 * or via kafka-console-consumer while debugging), so JSON removes the need for Vector
 * (or anything else) to hold a compiled protobuf descriptor set just to decode them,
 * while still keeping monitor-event.proto as the single source of truth for the shape
 * of the data.
 */
public class ProtobufJsonSerializer<T extends com.google.protobuf.Message> implements Serializer<T> {

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }

        try {
            // preservingProtoFieldNames() keeps e.g. "message_id" as written in the .proto,
            // instead of protobuf's default camelCase JSON mapping ("messageId") - Vector's
            // transform reads these fields by their .proto name.
            String json = JsonFormat.printer()
                .preservingProtoFieldNames()
                .omittingInsignificantWhitespace()
                .print(data);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to serialize protobuf message to JSON for topic: " + topic, ex);
        }
    }
}
