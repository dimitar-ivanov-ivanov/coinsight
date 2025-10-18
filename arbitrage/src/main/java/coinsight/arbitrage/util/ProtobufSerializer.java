package coinsight.arbitrage.util;

import org.apache.kafka.common.serialization.Serializer;

public class ProtobufSerializer<T extends com.google.protobuf.Message> implements Serializer<T> {

    @Override
    public byte[] serialize(final String topic, final T data) {
        return data == null ? null : data.toByteArray();
    }
}