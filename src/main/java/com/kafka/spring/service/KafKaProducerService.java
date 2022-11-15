package com.kafka.spring.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.spring.model.Vehicle;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
public class KafKaProducerService {
    private static final String LOG_PARSING_ERROR_ON_SUCCESS = "Parsing error on success";
    private static final String LOG_PARSING_ERROR_ON_FAILURE = "Parsing error on failure";
    private static final String LOG_VEHICLE_IS_SENT_WIHT_PARAMS_PATTERN =
            "'{}' is sent to Kafka: topic - '{}', partition - '{}', offset - '{}'";
    private static final String LOG_SENDING_FAILED_PATTERN = "Sending failed: %s";

    @Autowired
    private KafkaTemplate<String, Vehicle> vehicleKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, String> stringKafkaTemplate;

    @Autowired
    private VehicleService vehicleService;

    @Autowired
    private ObjectMapper objectMapper;

    @Value(value = "${input.topic.name}")
    private String inputTopicName;

    @Value(value = "${output.topic.name}")
    private String outputTopicName;


    public void sendMessageToOutputTopic(String key, String message) {
        ListenableFuture<SendResult<String, String>> future
                = this.stringKafkaTemplate.send(outputTopicName, key, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info(LOG_VEHICLE_IS_SENT_WIHT_PARAMS_PATTERN, message, result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error(String.format(LOG_SENDING_FAILED_PATTERN, message), ex);
            }
        });
    }

    public void sendVehicleToInputTopic(Vehicle vehicle) {
        vehicleService.saveVehicle(vehicle);
        ListenableFuture<SendResult<String, Vehicle>> future
                = this.vehicleKafkaTemplate.send(inputTopicName, vehicle.getVehicleId(), vehicle);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Vehicle>>() {

            @Override
            public void onSuccess(SendResult<String, Vehicle> result) {
                try {
                    log.info(LOG_VEHICLE_IS_SENT_WIHT_PARAMS_PATTERN, objectMapper.writeValueAsString(vehicle),
                            result.getRecordMetadata().topic(), result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } catch (JsonProcessingException ex) {
                    log.error(LOG_PARSING_ERROR_ON_SUCCESS, ex);
                }
            }

            @Override
            public void onFailure(Throwable ex) {
                try {
                    log.error(String.format(LOG_SENDING_FAILED_PATTERN, objectMapper.writeValueAsString(vehicle)), ex);
                } catch (JsonProcessingException exception) {
                    log.error(LOG_PARSING_ERROR_ON_FAILURE, exception);
                }
            }
        });
    }
}
