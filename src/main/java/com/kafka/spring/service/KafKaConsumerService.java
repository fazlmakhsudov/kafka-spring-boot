package com.kafka.spring.service;

import com.kafka.spring.model.Vehicle;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Service
@Slf4j
public class KafKaConsumerService {
    private static final int EXTENT = 2;

    @Setter
    private CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    private KafKaProducerService kafKaProducerService;

    @Autowired
    private VehicleService vehicleService;

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = "vehicleKafkaInputListenerContainerFactory")
    public void consumeByTrackerFirst(Vehicle vehicle) {
        log.info("Tracker consumer I proceeds to process: '{}'", vehicle);
        Double distance = vehicleService.countTotalDistance(vehicle);
//        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String message = String.format("%s has moved %.2f km", vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
        latch.countDown();
    }

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = "vehicleKafkaInputListenerContainerFactory")
    public void consumeByTrackerSecond(Vehicle vehicle) {
        log.info("Tracker consumer II proceeds to process: '{}'", vehicle);
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String message = String.format("%s has moved %.2f km", vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
        latch.countDown();
    }

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = "vehicleKafkaInputListenerContainerFactory")
    public void consumeByTrackerThird(Vehicle vehicle) {
        log.info("Tracker consumer III proceeds to process: '{}'", vehicle);
        double distance = sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        String message = String.format("%s has moved %.2f km", vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
        latch.countDown();
    }

    @KafkaListener(topics = "${output.topic.name}", groupId = "${output.topic.group.id}",
            containerFactory = "kafkaOutputListenerContainerFactory")
    public void consumeByLogger(String message) {
        log.info("Logger consumer receives: -> '{}'", message);
        latch.countDown();
    }

}
