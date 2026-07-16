package coinsight.arbitrage.streams.binance;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
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

    @Value("${kafka.streams.commitInterval}")
    private Integer windowSize;

    // If an event comes out of order and should be in an already closed window
    // If has "gracePeriod" time to arrive late at most before being discarded
    @Value("${kafka.streams.gracePeriod}")
    private Integer gracePeriod;

    @Autowired
    public void binanceStream(StreamsBuilder builder) {
        TimeWindows window = TimeWindows.ofSizeAndGrace(
                Duration.ofMillis(windowSize),
                Duration.ofMillis(gracePeriod));

        Serde<BinanceTickerOuterClass.BinanceTicker> serde
                = new BinanceTickerSerde();

        builder.stream(input, Consumed.with(Serdes.String(), serde))
                // Named explicitly (both here and in Materialized below) so the internal
                // repartition/changelog topics Kafka Streams creates get human-readable names
                .groupByKey(Grouped.<String, BinanceTickerOuterClass.BinanceTicker>as("binance-ticker-grouped")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(serde))
                .windowedBy(window)
                .reduce((oldVal, newVal) -> newVal,
                        Materialized.<String, BinanceTickerOuterClass.BinanceTicker, WindowStore<Bytes, byte[]>>as(
                                "binance-latest-reduce-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serde))
                // Without this, a windowed reduce is a continuously-updating table - every new
                // tick that updates a window's value is a candidate to be forwarded downstream
                // immediately (subject to commit-interval caching), not just once when the
                // window closes. This buffers every update and only forwards the single final
                // value once the window is genuinely closed - matching the README's/comment's
                // actual stated intent ("take the last value in the window") for the first time.
                .suppress(Suppressed.<Windowed<String>>untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("binance-latest-suppress"))
                .toStream()
                .selectKey((windowedKey, val) -> windowedKey.key())
                .to(output, Produced.with(Serdes.String(), serde));
    }
}
