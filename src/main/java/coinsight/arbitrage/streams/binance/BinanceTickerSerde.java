package coinsight.arbitrage.streams.binance;

import coinsight.arbitrage.shared.util.ProtobufDeserializer;
import coinsight.arbitrage.shared.util.ProtobufSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import ticker.BinanceTickerOuterClass;

public class BinanceTickerSerde implements Serde<BinanceTickerOuterClass.BinanceTicker> {

    @Override
    public Serializer<BinanceTickerOuterClass.BinanceTicker> serializer() {
        return new ProtobufSerializer<>();
    }

    @Override
    public Deserializer<BinanceTickerOuterClass.BinanceTicker> deserializer() {
        return new ProtobufDeserializer<>(BinanceTickerOuterClass.BinanceTicker.parser());
    }
}

