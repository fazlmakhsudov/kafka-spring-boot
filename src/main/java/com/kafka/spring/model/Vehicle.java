package com.kafka.spring.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.Digits;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class Vehicle {

    @JsonProperty("id")
    @NotBlank(message = "Vehicle Id is mandatory")
    private String vehicleId;

    @JsonProperty("x")
    @Min(value = 1, message = "'x' can't be less than 1")
    @Digits(integer = 2, message = "'x' shouldn't be more than 99", fraction = 0)
    private long abscissa;

    @JsonProperty("y")
    @Min(value = 1, message = "'y' can't be less than 1")
    @Digits(integer = 2, message = "'y' shouldn't be more than 99", fraction = 0)
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
