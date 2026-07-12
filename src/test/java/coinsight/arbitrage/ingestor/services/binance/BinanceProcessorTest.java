package coinsight.arbitrage.ingestor.services.binance;

import coinsight.arbitrage.ingestor.util.BinanceMapper;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import ticker.BinanceTickerOuterClass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BinanceProcessorTest {

    private static final int MESSAGE_ID = 1;

    private static final String CRYPTO_PAIR = "BTC:USDC";

    private static final String BINANCE_TOPIC = "binance-topic";

    private static final String BINANCE_DLT_TOPIC = "binance-dlt-topic";

    @InjectMocks
    private BinanceProcessor processor;

    @Mock
    private KafkaTemplate<String, BinanceTickerOuterClass.BinanceTicker> binanceTemplate;

    @Mock
    private KafkaTemplate<String, String> binanceDltTemplate;

    @Mock
    private BinanceMapper binanceMapper;

    @Mock
    private MonitoringService monitoringService;

    @BeforeEach
    void setUp() {
        processor = new BinanceProcessor();
        MockitoAnnotations.openMocks(this);
        ReflectionTestUtils.setField(processor, "binanceTopic", BINANCE_TOPIC);
        ReflectionTestUtils.setField(processor, "binanceDeadLetterTopic", BINANCE_DLT_TOPIC);
    }

    @Test
    void given_inputMessage_when_processMessage_then_publishToTopic() throws JsonProcessingException {
        // GIVEN
        String message = "msg";
        BinanceTickerOuterClass.BinanceTicker ticker = BinanceTickerOuterClass.BinanceTicker.newBuilder()
                .setMessageId(MESSAGE_ID).setCryptoPair(CRYPTO_PAIR)
                .build();

        when(binanceMapper.toBinanceTicker(message)).thenReturn(ticker);

        // WHEN
        processor.processMessage(message);

        // THEN
        verifyNoInteractions(binanceDltTemplate);
        verify(binanceTemplate).send(BINANCE_TOPIC, ticker.getCryptoPair(), ticker);
        verify(monitoringService).publishEvent("Binance Published message " + ticker.getMessageId(),
                "INFO", "ingestor");
    }

    @Test
    void given_nullMessage_when_processMessage_then_doNotPublishToTopic() throws JsonProcessingException {
        // GIVEN
        String message = "msg";
        when(binanceMapper.toBinanceTicker(message)).thenReturn(null);

        // WHEN
        processor.processMessage(message);

        // THEN
        verifyNoInteractions(binanceDltTemplate);
        verifyNoInteractions(binanceTemplate);
        verifyNoInteractions(monitoringService);
    }

    @Test
    void given_exception_when_onMessage_then_emitMessageToDeadLetterTopic() throws JsonProcessingException {
        // GIVEN
        String message = "msg";
        doThrow(JsonProcessingException.class).when(binanceMapper).toBinanceTicker(message);

        try {
            // WHEN
            processor.processMessage(message);
        } catch (Exception ex) {
            // THEN
            assertEquals(JsonProcessingException.class, ex.getClass());
            verify(monitoringService).publishEvent("Binance Error: " + ex.getMessage(),
                    "ERROR", "ingestor");
            verify(binanceDltTemplate).send(eq(BINANCE_DLT_TOPIC), anyString(), eq(message));
        }
    }
}