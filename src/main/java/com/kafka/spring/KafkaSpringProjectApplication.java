package com.kafka.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class KafkaSpringProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringProjectApplication.class, args);
	}

}
