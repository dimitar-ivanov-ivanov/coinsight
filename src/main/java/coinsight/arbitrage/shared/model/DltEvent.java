package coinsight.arbitrage.shared.model;

/**
 * Published to monitor-dlt-topic (raw JSON, same reasoning as {@link MonitorEvent})
 * when a MonitorEvent fails to publish.
 *
 * @param message the original raw message that failed to publish
 * @param severityLevel the original severity level
 * @param timestamp epoch millis of the failure
 * @param errorReason the exception message that caused the failure
 * @param messageId unique identifier (UUID) for this DLT event
 */
public record DltEvent(
    String message,
    String severityLevel,
    long timestamp,
    String errorReason,
    String messageId
) {
}
