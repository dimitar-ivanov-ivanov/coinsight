package coinsight.arbitrage.ingestor.services.binance;

import coinsight.arbitrage.ingestor.services.ExchangeProcessor;
import coinsight.arbitrage.ingestor.util.BinanceMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ticker.BinanceTickerOuterClass;

@Service
public class BinanceProcessor extends ExchangeProcessor<BinanceTickerOuterClass.BinanceTicker> {

    @Autowired
    private KafkaTemplate<String, BinanceTickerOuterClass.BinanceTicker> binanceTemplate;

    @Autowired
    private BinanceMapper binanceMapper;

    @Value("${kafka.binance.topic}")
    private String binanceTopic;

    @Autowired
    private KafkaTemplate<String, String> binanceDltTemplate;

    @Value("${kafka.binance.dlt.topic}")
    private String binanceDeadLetterTopic;

    @Override
    protected String getDltTopicName() {
        return binanceDeadLetterTopic;
    }

    @Override
    protected String getExchangeName() {
        return "Binance";
    }

    /**
     * Processor which is responsible for taking a raw input Binance message and
     * publishing a Kafka event in Protobuf format to the designated topic for that Binance.
     *
     * @param message input raw message from the Binance
     * @return event that was processed
     */
    @Override
    public BinanceTickerOuterClass.BinanceTicker process(String message) throws JsonProcessingException {
        BinanceTickerOuterClass.BinanceTicker ticker = binanceMapper.toBinanceTicker(message);
        if (ticker != null) {
            binanceTemplate.send(binanceTopic, ticker.getCryptoPair(), ticker);
            monitoringService.publishEvent("Binance Published message " + ticker.getMessageId(), "INFO");
        }

        return ticker;
    }
}
