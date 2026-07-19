package coinsight.arbitrage.ingestor.components;

import coinsight.arbitrage.ingestor.services.LeaderElectorService;
import coinsight.arbitrage.ingestor.services.coinbase.CoinbaseProcessor;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

@Component
public class CoinbaseWebSocketClient extends WebSocketClient {

    private static final String COINBASE_URL = "wss://advanced-trade-ws.coinbase.com";

    private static final List<String> STREAM_PARAMS = List.of("BTC-USDT", "ETH-USD", "XRP-USD", "SOL-USD");

    @Autowired
    private MonitoringService monitoringService;

    @Autowired
    private CoinbaseProcessor coinbaseProcessor;

    @Autowired
    private LeaderElectorService leaderElectorService;

    /**
     * Constructor for Coinbase web client.
     * Establishes a connection to Coinbase URL for streaming.
     *
     * @throws Exception if the URL is incorrect or Coinbase are down
     */
    public CoinbaseWebSocketClient() {
        super(URI.create(COINBASE_URL));
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        // send subscribe message
        send(buildSubscribeMessage());
        monitoringService.publishEvent("Connected to Coinbase WebSocket", "INFO", "ingestor");
    }

    @Override
    public void onMessage(String message) {
        if (leaderElectorService.isLeader()) {
            coinbaseProcessor.processMessage(message);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        String message = "Coinbase Connection closed" + reason + ", code:" + code;
        monitoringService.publishEvent(message, "INFO", "ingestor");
    }

    @Override
    public void onError(Exception e) {
        monitoringService.publishEvent("Coinbase WebSocket error: " + e.getMessage(), "ERROR", "ingestor");
    }

    private String buildSubscribeMessage() {
        StringBuilder builder = new StringBuilder("{\"type\":\"subscribe\", \"product_ids\": [");

        for (int i = 0; i < STREAM_PARAMS.size(); i++) {
            builder.append("\"").append(STREAM_PARAMS.get(i)).append("\"");

            if (notLastElement(i)) {
                builder.append(",");
            }
        }

        builder.append("], \"channel\": \"ticker\"}");
        return builder.toString();
    }

    private boolean notLastElement(int i) {
        return i != STREAM_PARAMS.size() - 1;
    }
}
