package com.kafka.spring.config;

import com.kafka.spring.model.Vehicle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    private static final String TRUSTED_PACKAGES = "*";

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${input.topic.group.id}")
    private String inputGroupId;

    @Value(value = "${output.topic.group.id}")
    private String outputGroupId;

    @Bean
    public ConsumerFactory<String, String> inputConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(createProps(inputGroupId));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaInputListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(inputConsumerFactory());
        return factory;
    }

    public ConsumerFactory<String, String> outputConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(createProps(outputGroupId));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaOutputListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(outputConsumerFactory());
        return factory;
    }

    // 2. Consume vehicle objects from Kafka
    public ConsumerFactory<String, Vehicle> vehicleConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, inputGroupId);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGES);
        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new JsonDeserializer<>(Vehicle.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Vehicle> vehicleKafkaInputListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Vehicle> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(vehicleConsumerFactory());
        return factory;
    }

    private Map<String, Object> createProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGES);
        return props;
    }
}
