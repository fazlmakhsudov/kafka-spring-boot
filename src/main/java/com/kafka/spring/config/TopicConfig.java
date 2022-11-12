package com.kafka.spring.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${input.topic.name}")
    private String inputTopicName;

    @Value(value = "${output.topic.name}")
    private String outputTopicName;

    @Value(value = "${kafka.partition.count}")
    private int partitionCount;

    @Value(value = "${kafka.replication-factor.count}")
    private int replicationFactorCount;

    public TopicConfig() {
    }

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name(inputTopicName)
                .partitions(partitionCount)
                .replicas(replicationFactorCount)
                .build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name(outputTopicName)
                .partitions(partitionCount)
                .replicas(replicationFactorCount)
                .build();
    }
}
