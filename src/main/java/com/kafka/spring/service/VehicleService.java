package com.kafka.spring.service;

import com.kafka.spring.entity.VehicleEntity;
import com.kafka.spring.model.Vehicle;
import com.kafka.spring.repository.VehicleRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

@Service
@Slf4j
public class VehicleService {
    private static final String LOG_VEHICLE_WAS_SAVED_PATTERN = "Vehicle was saved in database with id: '{}'";
    private static final String LOG_VEHICLE_WAS_UPDATED_PATTERN =
            "Vehicle was updated in database with id: '{}' and updated total distance: '{}' km";
    private static final String LOG_COVERED_DISTANCE_EQUAL_PATTERN =
            "New covered distance from previous coordinates is equal: '{}' km";
    private static final Double ZERO = 0d;
    private static final int EXTENT = 2;

    @Autowired
    private VehicleRepository repository;

    public void saveVehicle(Vehicle vehicle) {
        VehicleEntity entity = repository.findByVehicleId(vehicle.getVehicleId());
        if (Objects.isNull(entity)) {
            entity = new VehicleEntity();
            entity.setVehicleId(vehicle.getVehicleId());
            entity.setAbscissa(vehicle.getAbscissa());
            entity.setOrdinatus(vehicle.getOrdinatus());
            entity.setTotalDistance(ZERO);
            entity = repository.save(entity);
            log.info(LOG_VEHICLE_WAS_SAVED_PATTERN, entity.getId());
        }
    }

    public Double countTotalDistance(Vehicle vehicle) {
        VehicleEntity entity = repository.findByVehicleId(vehicle.getVehicleId());
        Double distance = countDistance(vehicle, entity);
        Double totalDistance = distance + entity.getTotalDistance();
        updateVehicle(entity, vehicle, totalDistance);
        log.info(LOG_COVERED_DISTANCE_EQUAL_PATTERN, distance);
        return totalDistance;
    }

    private void updateVehicle(VehicleEntity entity, Vehicle vehicle, Double totalDistance) {
        entity.setAbscissa(vehicle.getAbscissa());
        entity.setOrdinatus(vehicle.getOrdinatus());
        entity.setTotalDistance(totalDistance);
        entity = repository.save(entity);
        log.info(LOG_VEHICLE_WAS_UPDATED_PATTERN, entity.getId(),
                entity.getTotalDistance());
    }

    private static double countDistance(Vehicle vehicle, VehicleEntity entity) {
        if (Objects.isNull(entity.getTotalDistance()) || entity.getTotalDistance() <= ZERO) {
            return sqrt(pow(vehicle.getAbscissa(), EXTENT) + pow(vehicle.getOrdinatus(), EXTENT));
        }
        return sqrt(pow(vehicle.getAbscissa() - entity.getAbscissa(), EXTENT)
                + pow(vehicle.getOrdinatus() - entity.getOrdinatus(), EXTENT));
    }

}
