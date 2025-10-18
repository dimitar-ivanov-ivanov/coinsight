package coinsight.arbitrage.ingestor.services;

import coinsight.arbitrage.ingestor.util.MonitoringMapper;
import monitor.MonitorDtlEvent;
import monitor.MonitorEventOuterClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MonitoringServiceTest {

    private static final String MONITOR_TOPIC = "monitor-topic";

    private static final String DLT_MONITOR_TOPIC = "monitor-dlt-topic";

    @InjectMocks
    private MonitoringService monitoringService;

    @Mock
    private MonitoringMapper mapper;

    @Mock
    private KafkaTemplate<String, MonitorEventOuterClass.MonitorEvent> monitoringTemplate;

    @Mock
    private KafkaTemplate<String, MonitorDtlEvent.DltEvent> monitoringDltTemplate;

    @BeforeEach
    void setUp() {
        monitoringService = new MonitoringService();
        ReflectionTestUtils.setField(monitoringService, "monitoringTopic", MONITOR_TOPIC);
        ReflectionTestUtils.setField(monitoringService, "dltMonitoringTopic", DLT_MONITOR_TOPIC);
        MockitoAnnotations.openMocks(this);
    }

    @ParameterizedTest
    @MethodSource("invalidInput")
    void given_invalidInput_when_publishEvent_then_doNotPublish(String message, String severityLevel) {
        // GIVEN
        // WHEN
        monitoringService.publishEvent(message, severityLevel);
        // THEN
        verifyNoInteractions(mapper);
        verifyNoInteractions(monitoringTemplate);
        verifyNoInteractions(monitoringDltTemplate);
    }

    @Test
    void given_validInput_when_publishEvent_then_emitMessageToKafka() {
        // GIVEN
        String message = "msg";
        String severityLevel = "INFO";
        MonitorEventOuterClass.MonitorEvent event = MonitorEventOuterClass.MonitorEvent.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .build();
        when(mapper.toMonitoringEvent(message, severityLevel)).thenReturn(event);
        // WHEN
        monitoringService.publishEvent(message, severityLevel);
        // THEN
        verify(mapper).toMonitoringEvent(message, severityLevel);
        verify(monitoringTemplate).send(MONITOR_TOPIC, event.getMessageId(), event);
        verifyNoInteractions(monitoringDltTemplate);
    }

    @Test
    void given_error_when_publishToEvent_then_publishToDLT() {
        // GIVEN
        String message = "msg";
        String severityLevel = "INFO";
        Exception ex = new RuntimeException("BOOM");
        ArgumentCaptor<MonitorDtlEvent.DltEvent> dltCaptor = ArgumentCaptor.forClass(MonitorDtlEvent.DltEvent.class);
        when(mapper.toMonitoringEvent(anyString(), anyString())).thenThrow(ex);
        // WHEN
        monitoringService.publishEvent(message, severityLevel);
        // THEN
        verifyNoInteractions(monitoringTemplate);
        verify(monitoringDltTemplate).send(eq(DLT_MONITOR_TOPIC), dltCaptor.capture());

        MonitorDtlEvent.DltEvent event = dltCaptor.getValue();
        assertNotNull(event, "Published event to DLT should NOT be NULL.");
        assertEquals(message, event.getMessage());
        assertEquals(severityLevel, event.getSeverityLevel());
        assertEquals(ex.getMessage(), event.getErrorReason());
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