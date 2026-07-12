package coinsight.arbitrage.bff.socket;

import coinsight.arbitrage.shared.monitoring.MonitoringService;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Component
public class WebSocketListener {

    private final MonitoringService monitoringService;

    public WebSocketListener(MonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String sessionId = headerAccessor.getSessionId();

        monitoringService.publishEvent("New WebSocket connection established. SessionId: " + sessionId,
                "INFO", "ingestor");
    }

    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(event.getMessage());
        String sessionId = headerAccessor.getSessionId();

        monitoringService.publishEvent("WebSocket connection closed. SessionId: " + sessionId,
                "INFO", "ingestor");
    }
}
