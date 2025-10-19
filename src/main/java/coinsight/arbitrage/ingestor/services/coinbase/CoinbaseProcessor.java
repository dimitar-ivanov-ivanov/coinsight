package coinsight.arbitrage.ingestor.services.coinbase;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.ingestor.services.ExchangeProcessor;
import coinsight.arbitrage.ingestor.util.CoinbaseMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CoinbaseProcessor extends ExchangeProcessor<CoinbaseEvent.CoinbaseTicker> {

    @Autowired
    private CoinbaseMapper coinbaseMapper;

    @Value("${kafka.coinbase.topic}")
    private String coinbaseTopic;

    @Autowired
    private KafkaTemplate<String, CoinbaseEvent.CoinbaseTicker> coinbaseTemplate;

    @Autowired
    private KafkaTemplate<String, String> coinbaseDltTemplate;

    @Value("${kafka.coinbase.dlt.topic}")
    private String coinbaseDeadLetterTopic;

    @Override
    protected String getDltTopicName() {
        return coinbaseDeadLetterTopic;
    }

    @Override
    protected String getExchangeName() {
        return "Coinbase";
    }

    /**
     * Processor which is responsible for taking a raw input Coinbase message and
     * publishing a Kafka event in Protobuf format to the designated topic for that Coinbase.
     *
     * @param message input raw message from the Coinbase
     * @return event that was processed
     */
    @Override
    public CoinbaseEvent.CoinbaseTicker process(String message) throws JsonProcessingException {
        CoinbaseEvent.CoinbaseTicker ticker = coinbaseMapper.toCoinbaseTicker(message);
        if (ticker != null) {
            coinbaseTemplate.send(coinbaseTopic, ticker.getCryptoPair(), ticker);
            monitoringService.publishEvent("Coinbase Published message " + ticker.getMessageId(), "INFO");
        }
        return ticker;
    }
}
