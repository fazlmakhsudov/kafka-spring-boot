package com.kafka.spring.controller;

import com.kafka.spring.model.Vehicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.spring.service.KafKaProducerService;

@RestController
@RequestMapping(value = "/kafka/vehicle")
public class KafkaProducerController {

	private static Logger logger = LoggerFactory.getLogger(KafkaProducerController.class);

	private final KafKaProducerService producerService;

	@Autowired
	public KafkaProducerController(KafKaProducerService producerService) {
		this.producerService = producerService;
	}

	@PostMapping(value = "/publish-string")
	public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
		logger.info("Controller accepted message: {}", message);
	}


	@PostMapping(value = "/publish-object", consumes = "application/json")
	public void submit(@RequestBody Vehicle vehicle) {
		logger.info("Controller accepted vehicle: {}", vehicle);
		producerService.sendVehicleToInputTopic(vehicle);
	}

}