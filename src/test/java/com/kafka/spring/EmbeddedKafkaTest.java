package com.kafka.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.spring.model.Vehicle;
import com.kafka.spring.service.KafKaConsumerService;
import com.kafka.spring.service.KafKaProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import java.util.Objects;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@SpringBootTest(webEnvironment = WebEnvironment.MOCK, classes = {KafkaSpringProjectApplication.class})
@EmbeddedKafka(topics = {"${input.topic.name}", "${output.topic.name}"}, partitions = 3,
        brokerProperties = {"listeners=PLAINTEXT://${kafka.bootstrapAddress}", "port=${kafka.port}"})
public class EmbeddedKafkaTest {
    private static final String RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED = "Object was published";
    private static final String LOGGER_MESSAGE_PATTERN = "vehicle-1 has moved %.2f km";
    private static final String URL_PUBLISH_OBJECT = "/kafka/vehicle/publish";
    private static final String VEHICLE_1 = "vehicle-1";
    private static final int TIMEOUT = 2000;
    private static final int ORDINATUS = 4;
    private static final int ABSCISSA = 3;
    private static final int EXTENT = 2;
    private static final int ZERO = 0;
    private static final int ONE = 1;

    @Autowired
    private WebApplicationContext webApplicationContext;

    @SpyBean
    private KafKaConsumerService consumer;

    @SpyBean
    private KafKaProducerService producer;

    @Captor
    private ArgumentCaptor<Vehicle> vehicleArgumentCaptor;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;

    @Value("input.topic.name")
    private String inputTopic;

    @Value("output.topic.name")
    private String outputTopic;

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMvc;

    @BeforeEach
    public void setUp() {
        this.mockMvc = webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void testKafkaService() throws Exception {
        Vehicle vehicle = new Vehicle(VEHICLE_1, ABSCISSA, ORDINATUS);
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String vehicleJsonString = objectMapper.writeValueAsString(vehicle);

        String responseMessage = mockMvc.perform(post(URL_PUBLISH_OBJECT)
                .contentType(APPLICATION_JSON)
                .content(vehicleJsonString)
                .accept(APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        assertEquals(RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED, responseMessage);

        verify(producer, timeout(TIMEOUT).times(ONE))
                .sendVehicleToInputTopic(vehicleArgumentCaptor.capture());

        Vehicle vehicleCapturedFromProducer = vehicleArgumentCaptor.getValue();

        Vehicle vehicleCapturedByInputConsumer = verifyInputTopicConsumer();

        verify(consumer, timeout(TIMEOUT).times(ONE))
                .consumeByLogger(stringArgumentCaptor.capture());

        String loggerMessage = stringArgumentCaptor.getValue();

        assertNotNull(vehicleCapturedFromProducer);
        assertEquals(vehicle, vehicleCapturedFromProducer);
        assertNotNull(vehicleCapturedByInputConsumer);
        assertEquals(vehicle, vehicleCapturedByInputConsumer);
        assertNotNull(loggerMessage);
        assertEquals(String.format(LOGGER_MESSAGE_PATTERN, distance), loggerMessage);
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
