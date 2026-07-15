package coinsight.arbitrage.ingestor.services.binance;

import coinsight.arbitrage.ingestor.util.BinanceMapper;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import ticker.BinanceTickerOuterClass;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BinanceProcessorTest {

    private static final String MESSAGE_ID = UUID.randomUUID().toString();

    private static final String CRYPTO_PAIR = "BTC:USDC";

    private static final String BINANCE_TOPIC = "binance-topic";

    private static final String BINANCE_DLT_TOPIC = "binance-dlt-topic";

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
        MockitoAnnotations.openMocks(this);
        // Constructor call has to happen first - it fully replaces whatever @InjectMocks
        // might have wired up, so setting fields before this point was silently discarded.
        processor = new BinanceProcessor(binanceTemplate, binanceMapper, binanceDltTemplate);
        ReflectionTestUtils.setField(processor, "binanceTopic", BINANCE_TOPIC);
        ReflectionTestUtils.setField(processor, "binanceDeadLetterTopic", BINANCE_DLT_TOPIC);
        // monitoringService and dltTemplate are inherited from ExchangeProcessor and stay
        // field-injected there (Lombok's @RequiredArgsConstructor on BinanceProcessor only
        // covers BinanceProcessor's own fields, never a superclass's) - they still need to
        // be set manually here, same as any other field-injected dependency in a unit test.
        // Reusing the binanceDltTemplate mock for the inherited "dltTemplate" field too,
        // since that's the one actually invoked by ExchangeProcessor.processMessage()'s
        // catch block - the constructor's own binanceDltTemplate field is otherwise unused.
        ReflectionTestUtils.setField(processor, "monitoringService", monitoringService);
        ReflectionTestUtils.setField(processor, "dltTemplate", binanceDltTemplate);
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