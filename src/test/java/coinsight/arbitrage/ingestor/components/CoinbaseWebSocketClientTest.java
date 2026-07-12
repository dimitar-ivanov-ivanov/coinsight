package coinsight.arbitrage.ingestor.components;

import coinsight.arbitrage.ingestor.services.LeaderElectorService;
import coinsight.arbitrage.ingestor.services.coinbase.CoinbaseProcessor;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class CoinbaseWebSocketClientTest {

    @InjectMocks
    private CoinbaseWebSocketClient client;

    @Mock
    private MonitoringService monitoringService;

    @Mock
    private CoinbaseProcessor processor;

    @Mock
    private LeaderElectorService leaderElectorService;

    @BeforeEach
    void setUp() {
        client = spy(new CoinbaseWebSocketClient());
        doNothing().when(client).send(anyString());
        MockitoAnnotations.openMocks(this);
        doReturn(true).when(leaderElectorService).isLeader();
    }

    @Test
    void given_handshake_when_onOpen_then_emitMessageForSuccessfulOpening() {
        // GIVEN
        // WHEN
        client.onOpen(null);

        // THEN
        verify(monitoringService).publishEvent("Connected to Coinbase WebSocket", "INFO", "ingestor");
    }

    @Test
    void given_message_when_onMessage_then_useProcessor() {
        // GIVEN
        String message = "";

        // WHEN
        client.onMessage(message);

        // THEN
        verify(processor).processMessage(message);
        verifyNoInteractions(monitoringService);
    }

    @Test
    void given_codeAndReason_when_onClose_then_emitMonitoringMessage() {
        // GIVEN
        int code = 1;
        String reason = "closed";
        String expectedMessage = "Coinbase Connection closed" + reason + ", code:" + code;

        // WHEN
        client.onClose(code, reason, true);

        // THEN
        verify(monitoringService).publishEvent(expectedMessage, "INFO", "ingestor");
        verifyNoInteractions(processor);
    }

    @Test
    void given_exception_when_onOnError_then_emitMessageForError() {
        // GIVEN
        Exception ex = new Exception("test error");
        String expectedMessage = "Coinbase WebSocket error: " + ex.getMessage();

        // WHEN
        client.onError(ex);

        // THEN
        verifyNoInteractions(processor);
        verify(monitoringService).publishEvent(expectedMessage, "ERROR", "ingestor");
    }
}