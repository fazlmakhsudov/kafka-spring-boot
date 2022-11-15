package com.kafka.spring;

import com.kafka.spring.model.Vehicle;
import com.kafka.spring.service.KafKaConsumerService;
import com.kafka.spring.service.KafKaProducerService;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        classes = KafkaSpringProjectApplication.class,
        properties = "application-2.properties"
)
@EmbeddedKafka(topics = {"${input.topic.name}", "${output.topic.name}"}, partitions = 3,
        brokerProperties = {"listeners=PLAINTEXT://${kafka.bootstrapAddress}", "port=${kafka.port}"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(locations="classpath:application-2.properties")
class KafkaSpringProjectApplicationTests {
    private static final String RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED = "Object was published";
    private static final String LOGGER_MESSAGE_PATTERN = "%s has moved %.2f km";
    private static final String URL_PUBLISH_OBJECT = "/kafka/vehicle/publish";
    private static final int START_RANGE = 25;
    private static final int TIMEOUT = 2000;
    private static final int END_RANGE = 75;
    private static final int EXTENT = 2;
    private static final int ZERO = 0;

    @SpyBean
    private KafKaConsumerService consumer;

    @SpyBean
    private KafKaProducerService producer;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;

    @Captor
    private ArgumentCaptor<Vehicle> vehicleArgumentCaptor;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    void contextLoads() {
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("vehicles")
    public void testKafkaService(Vehicle vehicle) {
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));

        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(URL_PUBLISH_OBJECT, vehicle,
                String.class);

        assertThat(responseEntity.getBody())
                .isNotNull()
                .isEqualTo(RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED);

        verify(producer, timeout(TIMEOUT)).sendVehicleToInputTopic(vehicleArgumentCaptor.capture());

        Vehicle vehicleCapturedFromProducer = vehicleArgumentCaptor.getValue();

        Vehicle vehicleCapturedByInputConsumer = verifyInputTopicConsumer();

        verify(consumer, timeout(TIMEOUT)).consumeByLogger(stringArgumentCaptor.capture());

        String loggerMessage = stringArgumentCaptor.getValue();

        assertNotNull(vehicleCapturedFromProducer);
        assertEquals(vehicle, vehicleCapturedFromProducer);
        assertNotNull(vehicleCapturedByInputConsumer);
        assertEquals(vehicle, vehicleCapturedByInputConsumer);
        assertNotNull(loggerMessage);
        assertEquals(String.format(LOGGER_MESSAGE_PATTERN, vehicle.getVehicleId(), distance), loggerMessage);
    }

    private static Stream<Arguments> vehicles() {
        return IntStream.range(START_RANGE, END_RANGE)
                .mapToObj(id -> new Vehicle(Integer.toString(id), id, id))
                .map(Arguments::arguments);
    }

    private Vehicle verifyInputTopicConsumer() {
        verify(consumer, timeout(TIMEOUT).atLeast(ZERO))
                .consumeByTrackerFirst(vehicleArgumentCaptor.capture());

        Vehicle captured = vehicleArgumentCaptor.getValue();
        if (Objects.nonNull(captured)) {
            return captured;
        }

        verify(consumer, timeout(TIMEOUT).atLeast(ZERO))
                .consumeByTrackerSecond(vehicleArgumentCaptor.capture());

        captured = vehicleArgumentCaptor.getValue();
        if (Objects.nonNull(captured)) {
            return captured;
        }

        verify(consumer, timeout(TIMEOUT).atLeast(ZERO))
                .consumeByTrackerThird(vehicleArgumentCaptor.capture());

        captured = vehicleArgumentCaptor.getValue();
        if (Objects.nonNull(captured)) {
            return captured;
        }
        return null;
    }
}
