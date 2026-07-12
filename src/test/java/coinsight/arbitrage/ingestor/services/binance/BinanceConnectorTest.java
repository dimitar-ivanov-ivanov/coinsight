package coinsight.arbitrage.ingestor.services.binance;

import coinsight.arbitrage.ingestor.components.BinanceWebSocketClient;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

class BinanceConnectorTest {

    @Mock
    private BinanceWebSocketClient client;

    @Mock
    private MonitoringService monitoringService;

    @InjectMocks
    private BinanceConnector connector;

    @BeforeEach
    void setUp() {
        connector = new BinanceConnector();
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void given_startOfProgram_when_init_then_connectToWebSocket() {
        // GIVEN
        // WHEN
        connector.init();

        // THEN
        verify(client).connect();
        verify(monitoringService).publishEvent("Connecting to Binance WebSocket", "INFO", "ingestor");
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
        verify(monitoringService).publishEvent("Failed to initialize Binance WebSocket: " + ex.getMessage(), "INFO", "ingestor");
    }
}