package com.kafka.spring.repository;

import com.kafka.spring.entity.VehicleEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface VehicleRepository extends CrudRepository<VehicleEntity, Long> {

    VehicleEntity findByVehicleId(String vehicleId);

}
