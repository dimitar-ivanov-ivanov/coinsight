package coinsight.arbitrage.ingestor.services;

import coinsight.arbitrage.shared.model.MonitorEvent;
import coinsight.arbitrage.shared.monitoring.MonitoringMapper;
import coinsight.arbitrage.shared.monitoring.MonitoringService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MonitoringServiceTest {

    private static final String MONITOR_TOPIC = "monitor-topic";

    @InjectMocks
    private MonitoringService monitoringService;

    @Mock
    private MonitoringMapper mapper;

    @Mock
    private KafkaTemplate<String, MonitorEvent> monitoringTemplate;

    @BeforeEach
    void setUp() {
        monitoringService = new MonitoringService();
        ReflectionTestUtils.setField(monitoringService, "monitoringTopic", MONITOR_TOPIC);
        MockitoAnnotations. openMocks(this);
    }

    @ParameterizedTest
    @MethodSource("invalidInput")
    void given_invalidInput_when_publishEvent_then_doNotPublish(String message, String severityLevel) {
        // GIVEN
        // WHEN
        monitoringService.publishEvent(message, severityLevel, "ingestor");
        // THEN
        verifyNoInteractions(mapper);
        verifyNoInteractions(monitoringTemplate);
    }

    @Test
    void given_validInput_when_publishEvent_then_emitMessageToKafka() {
        // GIVEN
        String message = "msg";
        String severityLevel = "INFO";
        MonitorEvent event = new MonitorEvent(message, "", UUID.randomUUID().toString(),
                severityLevel, "ingestor", "", "", Map.of(), "", "");

        when(mapper.toMonitoringEvent(message, severityLevel, "ingestor")).thenReturn(event);
        // WHEN
        monitoringService.publishEvent(message, severityLevel, "ingestor");
        // THEN
        verify(mapper).toMonitoringEvent(message, severityLevel,"ingestor");
        verify(monitoringTemplate).send(MONITOR_TOPIC, event.messageId(), event);
    }

    private static Stream<Arguments> invalidInput() {
        return Stream.of(
                // null message
                Arguments.of(null, "INFO"),
                // null severity level
                Arguments.of("message", null),
                // both null
                Arguments.of(null, null)
        );
    }
}