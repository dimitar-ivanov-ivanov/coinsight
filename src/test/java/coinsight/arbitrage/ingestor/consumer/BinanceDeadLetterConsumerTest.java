package coinsight.arbitrage.ingestor.consumer;

import coinsight.arbitrage.ingestor.services.binance.BinanceProcessor;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import ticker.BinanceTickerOuterClass;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BinanceDeadLetterConsumerTest {

    private static final int MESSAGE_ID = 1;

    @InjectMocks
    private BinanceDeadLetterConsumer consumer;

    @Mock
    private BinanceProcessor processor;

    @Mock
    private MonitoringService monitoringService;

    @BeforeEach
    void setUp() {
        consumer = new BinanceDeadLetterConsumer();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void given_inputMessage_when_reprocessEvent_then_useBinanceProcessor() throws JsonProcessingException {
        // GIVEN
        String inputMessage = "";
        BinanceTickerOuterClass.BinanceTicker event = BinanceTickerOuterClass.BinanceTicker.newBuilder()
                .setMessageId(MESSAGE_ID)
                .build();
        String monitoringMessage = "[DLT] Successfully republished message with id " + event.getMessageId();
        when(processor.process(inputMessage)).thenReturn(event);
        // WHEN
        consumer.reprocessEvent(inputMessage);
        // THEN
        verify(monitoringService).publishEvent(monitoringMessage, "INFO", "ingestor");
        verify(processor).process(inputMessage);
    }

    @Test
    void given_invalidMessage_when_reprocessEvent_then_emitMessageForInvalidInput() throws JsonProcessingException {
        // GIVEN
        String inputMessage = "";
        when(processor.process(inputMessage)).thenReturn(null);
        // WHEN
        consumer.reprocessEvent(inputMessage);
        // THEN
        verify(monitoringService).publishEvent(anyString(), eq("ERROR"), eq("ingestor"));
        verify(processor).process(inputMessage);
    }

}