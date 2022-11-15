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

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/kafka/vehicle")
@Slf4j
public class KafkaProducerController {
    private static final String LOG_CONTROLLER_ACCEPTED_VEHICLE = "Controller accepted vehicle: {}";
    private static final String RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED = "Object was published";
    private static final String LOG_WAITING_LASTS_LONG = "Waiting lasts long";
    private static final String URL_PUBLISH_OBJECT = "/publish-object";

    @Autowired
    private KafKaProducerService producerService;

    @Autowired
    private KafKaConsumerService consumerService;

    @PostMapping(value = URL_PUBLISH_OBJECT, consumes = APPLICATION_JSON_VALUE)
    public String sendVehicleToKafkaTopic(@Valid @RequestBody Vehicle vehicle) {
        log.info(LOG_CONTROLLER_ACCEPTED_VEHICLE, vehicle);
        producerService.sendVehicleToInputTopic(vehicle);
        CountDownLatch latch = new CountDownLatch(1);
        consumerService.setLatch(latch);
        try {
            latch.await();
        } catch (InterruptedException ex) {
            log.error(LOG_WAITING_LASTS_LONG, ex);
        }
        return RESPONSE_MESSAGE_OBJECT_WAS_PUBLISHED;
    }

}