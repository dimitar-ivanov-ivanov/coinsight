package coinsight.arbitrage.shared.model;

import java.util.Map;

/**
 * Event published to monitor-topic, serialized directly to JSON (Spring Kafka's
 * {@code JsonSerializer}) - not protobuf.
 *
 * <p>There used to be a monitor-event.proto for this, with the protobuf message
 * converted to JSON before publishing. That added a schema-generation step for no
 * benefit: monitor-topic is low volume and exists to be read (Grafana, or
 * kafka-console-consumer while debugging), not replayed or consumed by another
 * service in binary form. It also had a real footgun - proto3's JSON mapping can't
 * distinguish "unset" from "default value", so unset/default fields (an empty
 * metadata map, an empty trace id) were silently omitted from the JSON instead of
 * appearing as {@code {}}/{@code ""}. A plain record has no such ambiguity.
 *
 * @param message plain log message
 * @param timestamp ISO8601 timestamp
 * @param messageId unique identifier (UUID) for this event
 * @param level severity, e.g. "INFO", "ERROR", "WARN", "DEBUG"
 * @param serviceName e.g. "ingestor"
 * @param instanceId unique per service instance
 * @param traceId for tracing (OpenTelemetry/Jaeger)
 * @param metadata arbitrary key-value metadata (tags)
 * @param host hostname or IP of the emitting service
 * @param environment e.g. "prod", "dev", "staging"
 */
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
