package coinsight.arbitrage.streams.binance;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ticker.BinanceTickerOuterClass;

import java.time.Duration;

@Component
public class BinanceStream {

    @Value("${kafka.binance.topic}")
    private String input;

    @Value("${kafka.binance.latest.topic}")
    private String output;

    @Value("${kafka.streams.binance.commitInterval}")
    private Integer windowSize;

    // If an event comes out of order and should be in an already closed window
    // If has "gracePeriod" time to arrive late at most before being discarded
    @Value("${kafka.streams.binance.gracePeriod}")
    private Integer gracePeriod;

    @Autowired
    public void binanceStream(StreamsBuilder builder) {
        TimeWindows window = TimeWindows.ofSizeAndGrace(
                Duration.ofMillis(windowSize),
                Duration.ofSeconds(gracePeriod));

        Serde<BinanceTickerOuterClass.BinanceTicker> serde
                = new BinanceTickerSerde();

        builder.stream(input, Consumed.with(Serdes.String(), serde))
                .groupByKey()
                .windowedBy(window)
                // Take the last value in the given window
                .reduce((oldVal, newVal) -> newVal)
                .toStream()
                .selectKey((windowedKey, val) -> windowedKey.key())
                .to(output, Produced.with(Serdes.String(), serde));
    }
}
