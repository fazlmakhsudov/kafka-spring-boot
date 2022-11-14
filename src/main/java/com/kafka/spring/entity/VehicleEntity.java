package com.kafka.spring.entity;

import lombok.Data;
import lombok.ToString;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.validation.constraints.NotNull;
import java.util.Date;

import static javax.persistence.GenerationType.IDENTITY;

@Entity
@Data
@ToString
public class VehicleEntity {

    @Id
    @GeneratedValue(strategy = IDENTITY)
    private Long id;

    @Column(name = "vehicle_id")
    @NotNull
    private String vehicleId;

    @Column(name = "x")
    @NotNull
    private Long abscissa;

    @Column(name = "y")
    @NotNull
    private Long ordinatus;

    @Column(name = "total_distance")
    @NotNull
    private Double totalDistance;

    @CreatedDate
    @Column(name = "created_date")
    private Date createdAt;

    @LastModifiedDate
    @Column(name = "modified_date")
    private Date modifiedAt;
}
