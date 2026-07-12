package coinsight.arbitrage.ingestor.services.binance;

import coinsight.arbitrage.ingestor.components.BinanceWebSocketClient;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
@RequiredArgsConstructor
public class BinanceConnector {

    private final BinanceWebSocketClient webSocketClient;

    private final MonitoringService monitoringService;

    /**
     * On initialization open a websocket to the Binance stream.
     * Instead of using logger, send the messages to the monitor topic.
     * The monitoring service will take care of them.
     */
    @PostConstruct
    public void init() {
        try {
            webSocketClient.connect();
            monitoringService.publishEvent("Connecting to Binance WebSocket", "INFO", "ingestor");
        } catch (Exception e) {
            monitoringService.publishEvent("Failed to initialize Binance WebSocket: " + e.getMessage(), "INFO", "ingestor");
        }
    }
}
