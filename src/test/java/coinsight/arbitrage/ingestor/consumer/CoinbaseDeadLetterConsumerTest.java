package coinsight.arbitrage.ingestor.consumer;

import coinbase.ticker.CoinbaseEvent;
import coinsight.arbitrage.ingestor.services.coinbase.CoinbaseProcessor;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoinbaseDeadLetterConsumerTest {

    @InjectMocks
    private CoinbaseDeadLetterConsumer consumer;

    @Mock
    private CoinbaseProcessor processor;

    @Mock
    private MonitoringService monitoringService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        consumer = new CoinbaseDeadLetterConsumer(processor, monitoringService);
    }

    @Test
    void given_inputMessage_when_reprocessEvent_then_useCoinbaseProcessor() throws JsonProcessingException {
        // GIVEN
        String inputMessage = "";
        CoinbaseEvent.CoinbaseTicker event = CoinbaseEvent.CoinbaseTicker.newBuilder()
                .setMessageId(100)
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