package coinsight.arbitrage.ingestor.services.coinbase;

import coinsight.arbitrage.ingestor.components.CoinbaseWebSocketClient;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class CoinbaseConnector {

    @Autowired
    private CoinbaseWebSocketClient client;

    @Autowired
    private MonitoringService monitoringService;

    /**
     * On initialization open a websocket to the Coinbase stream.
     * Instead of using logger, send the messages to the monitor topic.
     * The monitoring service will take care of them.
     */
    @PostConstruct
    public void init() {
        try {
            client.connect();
            monitoringService.publishEvent("Connecting to Coinbase WebSocket", "INFO", "ingestor");
        } catch (Exception e) {
            monitoringService.publishEvent("Failed to initialize Coinbase WebSocket: " + e.getMessage(), "INFO", "ingestor");
        }
    }
}
