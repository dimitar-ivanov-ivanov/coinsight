package coinsight.arbitrage.streams.coinbase;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.shared.util.ProtobufDeserializer;
import coinsight.arbitrage.shared.util.ProtobufSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CoinbaseTickerSerde implements Serde<CoinbaseEvent.CoinbaseTicker> {

    @Override
    public Serializer<CoinbaseEvent.CoinbaseTicker> serializer() {
        return new ProtobufSerializer<>();
    }

    @Override
    public Deserializer<CoinbaseEvent.CoinbaseTicker> deserializer() {
        return new ProtobufDeserializer<>(CoinbaseEvent.CoinbaseTicker.parser());
    }
}
