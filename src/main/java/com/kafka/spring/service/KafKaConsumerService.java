package com.kafka.spring.service;

import com.kafka.spring.model.Vehicle;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
@Slf4j
public class KafKaConsumerService {
    private static final String LOG_TRACKER_CONSUMER_PROCESS_PATTERN = "Tracker consumer %S proceeds to process: '%s'";
    private static final String VEHICLE_INPUT_LISTENER_FACTORY = "vehicleKafkaInputListenerContainerFactory";
    private static final String STRING_KAFKA_OUTPUT_LISTENER_FACTORY = "kafkaOutputListenerContainerFactory";
    private static final String LOG_LOGGING_CONSUMER_RECEIVES_PATTERN = "Logger consumer receives: -> '{}'";
    private static final String VEHICLE_MOVED_DISTANCE_PATTERN = "%s has moved %.2f km";
    private static final String ROMAN_THREE = "III";
    private static final String ROMAN_TWO = "II";
    private static final String ROMAN_ONE = "I";

    @Autowired
    private KafKaProducerService kafKaProducerService;

    @Autowired
    private VehicleService vehicleService;

    @Setter
    private CountDownLatch latch = new CountDownLatch(1);


    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = VEHICLE_INPUT_LISTENER_FACTORY)
    public void consumeByTrackerFirst(Vehicle vehicle) {
        log.info(String.format(LOG_TRACKER_CONSUMER_PROCESS_PATTERN, ROMAN_ONE, vehicle));
        Double distance = vehicleService.countTotalDistance(vehicle);
        String message = String.format(VEHICLE_MOVED_DISTANCE_PATTERN, vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
        latch.countDown();
    }

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = VEHICLE_INPUT_LISTENER_FACTORY)
    public void consumeByTrackerSecond(Vehicle vehicle) {
        log.info(String.format(LOG_TRACKER_CONSUMER_PROCESS_PATTERN, ROMAN_TWO, vehicle));
        Double distance = vehicleService.countTotalDistance(vehicle);
        String message = String.format(VEHICLE_MOVED_DISTANCE_PATTERN, vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
        latch.countDown();
    }

    @KafkaListener(topics = "${input.topic.name}", groupId = "${input.topic.group.id}",
            containerFactory = VEHICLE_INPUT_LISTENER_FACTORY)
    public void consumeByTrackerThird(Vehicle vehicle) {
        log.info(String.format(LOG_TRACKER_CONSUMER_PROCESS_PATTERN, ROMAN_THREE, vehicle));
        Double distance = vehicleService.countTotalDistance(vehicle);
        String message = String.format(VEHICLE_MOVED_DISTANCE_PATTERN, vehicle.getVehicleId(), distance);
        kafKaProducerService.sendMessageToOutputTopic(vehicle.getVehicleId(), message);
        latch.countDown();
    }

    @KafkaListener(topics = "${output.topic.name}", groupId = "${output.topic.group.id}",
            containerFactory = STRING_KAFKA_OUTPUT_LISTENER_FACTORY)
    public void consumeByLogger(String message) {
        log.info(LOG_LOGGING_CONSUMER_RECEIVES_PATTERN, message);
        latch.countDown();
    }

}
