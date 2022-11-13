package com.kafka.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.spring.KafkaSpringProjectApplication;
import com.kafka.spring.model.Vehicle;
import com.kafka.spring.service.KafKaConsumerService;
import com.kafka.spring.service.KafKaProducerService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment=WebEnvironment.MOCK, classes={ KafkaSpringProjectApplication.class })
@EmbeddedKafka(topics = { "${input.topic.name}","${output.topic.name}" }, partitions = 3,
        brokerProperties = { "listeners=PLAINTEXT://${kafka.bootstrapAddress}",
                "port=${kafka.port}" })
public class EmbeddedKafkaTest {

    private static final String URL_PUBLISH_STRING = "/kafka/vehicle/publish-string";
    private static final String URL_PUBLISH_OBJECT = "/kafka/vehicle/publish-object";
    private static final String RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED = "Object was published";
    private static final String RESPONSE_MESSAGE_STRING_WAS_PUBLISHED = "Message was published";

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext webApplicationContext;

    @SpyBean
    private KafKaConsumerService consumer;

    @SpyBean
    private KafKaProducerService producer;
    @Captor
    private ArgumentCaptor<Vehicle> vehicleArgumentCaptor;

    @Value("input.topic.name")
    private String inputTopic;

    @Value("output.topic.name")
    private String outputTopic;

    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        this.mockMvc = webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void testPublishStringEndpoint() throws Exception {
        String responseMessage = mockMvc.perform(post(URL_PUBLISH_STRING)
                .contentType(MediaType.TEXT_PLAIN)
                .content("Just simple message")
                .accept(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(RESPONSE_MESSAGE_STRING_WAS_PUBLISHED, responseMessage);
    }

    @Test
    public void testProducer() throws Exception {
        Vehicle vehicle = new Vehicle("vehicle-1", 3,4);
        String vehicleJsonString = objectMapper.writeValueAsString(vehicle);

        String responseMessage = mockMvc.perform(post(URL_PUBLISH_OBJECT)
                .contentType(APPLICATION_JSON)
                .content(vehicleJsonString)
                .accept(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED, responseMessage);

        verify(producer, timeout(2000).times(1))
                .sendVehicleToInputTopic(vehicleArgumentCaptor.capture());

        Vehicle vehicleCaptured = vehicleArgumentCaptor.getValue();

        assertNotNull(vehicleCaptured);
        assertEquals(vehicle, vehicleCaptured);
    }

}
