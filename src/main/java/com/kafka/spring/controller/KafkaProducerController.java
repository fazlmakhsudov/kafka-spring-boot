package com.kafka.spring.controller;

import com.kafka.spring.model.Vehicle;
import com.kafka.spring.service.KafKaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping(value = "/kafka/vehicle")
public class KafkaProducerController {

    private static final String RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED = "Object was published";
    private static final String RESPONSE_MESSAGE_STRING_WAS_PUBLISHED = "Message was published";
    private Logger logger = LoggerFactory.getLogger(KafkaProducerController.class);

    private final KafKaProducerService producerService;

    @Autowired
    public KafkaProducerController(KafKaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping(value = "/publish-string")
    public String sendMessageToKafkaTopic(@RequestBody String message) {
        logger.info("Controller accepted message: {}", message);
        return RESPONSE_MESSAGE_STRING_WAS_PUBLISHED;
    }


    @PostMapping(value = "/publish-object", consumes = "application/json")
    public String sendVehicleToKafkaTopic(@Valid @RequestBody Vehicle vehicle) {
        logger.info("Controller accepted vehicle: {}", vehicle);
        producerService.sendVehicleToInputTopic(vehicle);
        return RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED;
    }

}