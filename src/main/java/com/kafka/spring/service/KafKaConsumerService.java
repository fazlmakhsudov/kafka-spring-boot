package com.kafka.spring.service;

import com.kafka.spring.model.Vehicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Service
public class KafKaConsumerService {

    private final Logger logger = LoggerFactory.getLogger(KafKaConsumerService.class);
    private static final int EXTENT = 2;

    @Autowired
    private KafKaProducerService kafKaProducerService;

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = "vehicleKafkaInputListenerContainerFactory")
    public void consume(Vehicle vehicle) {
        logger.info("Tracker consumer I proceeds to process: '{}'", vehicle);
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String message = String.format("%s has moved %f km", vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
    }

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = "vehicleKafkaInputListenerContainerFactory")
    public void consumeByTrackerSecond(Vehicle vehicle) {
        logger.info("Tracker consumer II proceeds to process: '{}'", vehicle);
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String message = String.format("%s has moved %f km", vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
    }

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = "vehicleKafkaInputListenerContainerFactory")
    public void consumeByTrackerThird(Vehicle vehicle) {
        logger.info("Tracker consumer III proceeds to process: '{}'", vehicle);
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String message = String.format("%s has moved %.2f km", vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
    }

    @KafkaListener(topics = "${output.topic.name}", groupId = "${output.topic.group.id}",
            containerFactory = "kafkaOutputListenerContainerFactory")
    public void consumeByLogger(String message) {
        logger.info("Logger consumer receives: -> '{}'", message);
    }

}
