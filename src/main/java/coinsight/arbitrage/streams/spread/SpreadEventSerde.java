package coinsight.arbitrage.streams.spread;

import coinsight.arbitrage.shared.util.ProtobufDeserializer;
import coinsight.arbitrage.shared.util.ProtobufSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import spread.SpreadEventOuterClass;

public class SpreadEventSerde implements Serde<SpreadEventOuterClass.SpreadEvent> {

    @Override
    public Serializer<SpreadEventOuterClass.SpreadEvent> serializer() {
        return new ProtobufSerializer<>();
    }

    @Override
    public Deserializer<SpreadEventOuterClass.SpreadEvent> deserializer() {
        return new ProtobufDeserializer<>(SpreadEventOuterClass.SpreadEvent.parser());
    }
}
