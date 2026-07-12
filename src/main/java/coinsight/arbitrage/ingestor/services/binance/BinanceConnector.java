package coinsight.arbitrage.ingestor.services.binance;

import coinsight.arbitrage.ingestor.components.BinanceWebSocketClient;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class BinanceConnector {

    @Autowired
    private BinanceWebSocketClient webSocketClient;

    @Autowired
    private MonitoringService monitoringService;

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
