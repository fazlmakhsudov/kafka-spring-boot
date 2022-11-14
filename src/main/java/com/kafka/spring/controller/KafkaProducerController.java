package com.kafka.spring.controller;

import com.kafka.spring.model.Vehicle;
import com.kafka.spring.service.KafKaConsumerService;
import com.kafka.spring.service.KafKaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.concurrent.CountDownLatch;

@RestController
@RequestMapping(value = "/kafka/vehicle")
@Slf4j
public class KafkaProducerController {
    private static final String RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED = "Object was published";
    private static final String RESPONSE_MESSAGE_STRING_WAS_PUBLISHED = "Message was published";

    private final KafKaProducerService producerService;
    private final KafKaConsumerService consumerService;

    @Autowired
    public KafkaProducerController(KafKaProducerService producerService, KafKaConsumerService consumerService) {
        this.producerService = producerService;
        this.consumerService = consumerService;
    }

    @PostMapping(value = "/publish-string")
    public String sendMessageToKafkaTopic(@RequestBody String message) {
        log.info("Controller accepted message: {}", message);
        return RESPONSE_MESSAGE_STRING_WAS_PUBLISHED;
    }


    @PostMapping(value = "/publish-object", consumes = "application/json")
    public String sendVehicleToKafkaTopic(@Valid @RequestBody Vehicle vehicle) {
        log.info("Controller accepted vehicle: {}", vehicle);
        producerService.sendVehicleToInputTopic(vehicle);
        CountDownLatch latch = new CountDownLatch(1);
        consumerService.setLatch(latch);
        try {
            latch.await();
        } catch (InterruptedException ex) {
            log.error("Waiting lasts long", ex);
        }
        return RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED;
    }

}