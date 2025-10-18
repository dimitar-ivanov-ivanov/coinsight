package coinsight.arbitrage.ingestor.services.coinbase;

import coinsight.arbitrage.ingestor.components.CoinbaseWebSocketClient;
import coinsight.arbitrage.ingestor.services.MonitoringService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

class CoinbaseConnectorTest {

    @Mock
    private CoinbaseWebSocketClient client;

    @Mock
    private MonitoringService monitoringService;

    @InjectMocks
    private CoinbaseConnector connector;

    @BeforeEach
    void setUp() {
        connector = new CoinbaseConnector();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void given_startOfProgram_when_init_then_connectToWebSocket() {
        // GIVEN
        // WHEN
        connector.init();

        // THEN
        verify(client).connect();
        verify(monitoringService).publishEvent("Connecting to Coinbase WebSocket", "INFO");
    }

    @Test
    void given_exception_when_init_then_publishEventToMonitoringTopic() {
        // GIVEN
        RuntimeException ex = new RuntimeException("boom");
        doThrow(ex).when(client).connect();

        // WHEN
        connector.init();

        // THEN
        verify(client).connect();
        verify(monitoringService).publishEvent("Failed to initialize Coinbase WebSocket: " + ex.getMessage(), "INFO");
    }
}