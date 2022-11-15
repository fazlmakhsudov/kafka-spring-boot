package com.kafka.spring;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"testTopic"}, partitions = 3,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9093", "port=9093"})
@SpringBootTest(classes = KafkaSpringProjectApplication.class)
public class SingleConsumerProducerKafkaTest {
    private static final String MY_TEST_VALUE = "my-test-value";
    private static final String TEST_TOPIC = "testTopic";
    private static final String TEST_GROUP = "testGroup";
    private static final String EARLIEST = "earliest";
    private static final String TRUE = "true";
    private static final int KEY = 123;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testReceivingKafkaEvents() {
        Consumer<Integer, String> consumer = configureConsumer();
        Producer<Integer, String> producer = configureProducer();

        producer.send(new ProducerRecord<>(TEST_TOPIC, KEY, MY_TEST_VALUE));

        ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TEST_TOPIC);
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo(KEY);
        assertThat(singleRecord.value()).isEqualTo(MY_TEST_VALUE);

        consumer.close();
        producer.close();
    }

    private Consumer<Integer, String> configureConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(TEST_GROUP, TRUE,
                embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        Consumer<Integer, String> consumer = new DefaultKafkaConsumerFactory<Integer, String>(consumerProps)
                .createConsumer();
        consumer.subscribe(Collections.singleton(TEST_TOPIC));
        return consumer;
    }

    private Producer<Integer, String> configureProducer() {
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        return new DefaultKafkaProducerFactory<Integer, String>(producerProps).createProducer();
    }
}