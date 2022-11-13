package com.kafka.spring.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class Vehicle {

    @JsonProperty("id")
    private String vehicleId;

    @JsonProperty("x")
    private long abscissa;

    @JsonProperty("y")
    private long ordinatus;

    @Override
    public String toString() {
        return "{" +
                "id='" + vehicleId + '\'' +
                ", x=" + abscissa +
                ", y=" + ordinatus +
                '}';
    }
}
