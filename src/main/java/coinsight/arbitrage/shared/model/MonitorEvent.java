package coinsight.arbitrage.shared.model;

import java.util.Map;

public record MonitorEvent(
    String message,
    String timestamp,
    String messageId,
    String level,
    String serviceName,
    String instanceId,
    String traceId,
    Map<String, String> metadata,
    String host,
    String environment
) {
}
