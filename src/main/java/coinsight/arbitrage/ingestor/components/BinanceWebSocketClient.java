package coinsight.arbitrage.ingestor.components;

import coinsight.arbitrage.ingestor.services.LeaderElectorService;
import coinsight.arbitrage.ingestor.services.binance.BinanceProcessor;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

@Component
public class BinanceWebSocketClient extends WebSocketClient {

    private static final String BINANCE_URL = "wss://stream.binance.com:9443/ws";

    private static final List<String> STREAM_PARAMS
        = List.of("btcusdc@ticker",
            "btcusdt@ticker",
            "ethusdt@ticker",
            "solusdt@ticker",
            "yoyobtc@ticker",
            "sysbtc@ticker",
            "qlcbnb@ticker",
            "ethusdc@ticker",
            "solusdt@ticker",
            "bnbusdt@ticker",
            "xrpusdt@ticke");

    @Autowired
    private BinanceProcessor binanceProcessor;

    @Autowired
    private MonitoringService monitoringService;

    @Autowired
    private LeaderElectorService leaderElectorService;

    /**
     * Constructor for Binance web client.
     * Establishes a connection to Binance URL for streaming.
     *
     */
    public BinanceWebSocketClient() {
        super(URI.create(BINANCE_URL));
    }

    /**
     * Subscribe to Binance web socket.
     *
     * @param handshake the server handshake
     */
    @Override
    public void onOpen(ServerHandshake handshake) {
        String subscribeMessage = buildSubscribeMessage();
        send(subscribeMessage);

        monitoringService.publishEvent("Connected to Binance WebSocket", "INFO", "ingestor");
    }

    /**
     * Handle Binance message by publishing to our dedicated Binance Kafka topic.
     *
     * @param message input message
     */
    @Override
    public void onMessage(String message) {
        if (leaderElectorService.isLeader()) {
            binanceProcessor.processMessage(message);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        String message = "Binance Connection closed" + reason + ", code:" + code;
        monitoringService.publishEvent(message, "INFO", "ingestor");
    }

    /**
     * After multiple errors occur the Circuit breaker activates and stops consumption from
     * the Binance websocket.
     *
     * @param ex exception thrown
     */
    @Override
    public void onError(Exception ex) {
        monitoringService.publishEvent("Binance WebSocket error: " + ex.getMessage(), "ERROR", "ingestor");
    }

    /**
     * Build subscribe message for Binance Websocket.
     *
     * @return subscribe message
     */
    private String buildSubscribeMessage() {
        StringBuilder builder = new StringBuilder("{\"method\":\"SUBSCRIBE\",\"params\":[");

        for (int i = 0; i < STREAM_PARAMS.size(); i++) {
            builder.append("\"").append(STREAM_PARAMS.get(i)).append("\"");

            if (notLastElement(i)) {
                builder.append(",");
            }
        }

        builder.append("],\"id\":1}");
        return builder.toString();
    }

    private boolean notLastElement(int i) {
        return i != STREAM_PARAMS.size() - 1;
    }
}
