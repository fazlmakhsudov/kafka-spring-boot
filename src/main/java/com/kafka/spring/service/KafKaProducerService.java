package com.kafka.spring.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.spring.model.Vehicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafKaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafKaProducerService.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Vehicle> vehicleKafkaTemplate;

    @Value(value = "${input.topic.name}")
    private String inputTopicName;

    @Value(value = "${output.topic.name}")
    private String outputTopicName;

    public void sendMessageToInputTopic(String message) {
        ListenableFuture<SendResult<String, String>> future
                = this.kafkaTemplate.send(inputTopicName, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("+	Sent message: " + message
                        + " with offset: " + result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("+	Unable to send message : " + message, ex);
            }
        });
    }

    public void sendMessageToOutputTopic(String key, String message) {
        ListenableFuture<SendResult<String, String>> future
                = this.kafkaTemplate.send(outputTopicName, key, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("'{}' is sent to Kafka: topic - '{}', partition - '{}', offset - '{}'", message,
                        result.getRecordMetadata().topic(), result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("Unable to send message : " + message, ex);
            }
        });
    }

    public void sendVehicleToInputTopic(Vehicle vehicle) {
        ListenableFuture<SendResult<String, Vehicle>> future
                = this.vehicleKafkaTemplate.send(inputTopicName, vehicle.getVehicleId(), vehicle);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Vehicle>>() {

            @Override
            public void onSuccess(SendResult<String, Vehicle> result) {
                try {
                    logger.info("'{}' is sent to Kafka: topic - '{}', partition - '{}', offset - '{}'",
                            objectMapper.writeValueAsString(vehicle), result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                } catch (JsonProcessingException ex) {
                    logger.error("Parsing error on success", ex);
                }
            }

            @Override
            public void onFailure(Throwable ex) {
                try {
                    logger.error("Sending failed: " + objectMapper.writeValueAsString(vehicle), ex);
                } catch (JsonProcessingException exception) {
                    logger.error("Parsing error on failure", exception);
                }
            }
        });
    }

    public void sendVehicleToOutputTopic(Vehicle vehicle) {
        ListenableFuture<SendResult<String, Vehicle>> future
                = this.vehicleKafkaTemplate.send(outputTopicName, vehicle.getVehicleId(), vehicle);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Vehicle>>() {

            @Override
            public void onSuccess(SendResult<String, Vehicle> result) {
                try {
                    logger.info("{} is sent to Kafka: topic - '{}', partition - '{}', offset - '{}'",
                            objectMapper.writeValueAsString(vehicle), result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                } catch (JsonProcessingException ex) {
                    logger.error("Parsing error on success", ex);
                }
            }

            @Override
            public void onFailure(Throwable ex) {
                try {
                    logger.error(objectMapper.writeValueAsString(vehicle), ex);
                } catch (JsonProcessingException exception) {
                    logger.error("Parsing error on failure", exception);
                }
            }
        });
    }
}
