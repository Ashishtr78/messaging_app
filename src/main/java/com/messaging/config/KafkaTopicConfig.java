package com.messaging.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the chat topic so it is auto-created on startup in dev.
 * In a managed prod cluster topics are usually provisioned by infra, not the app,
 * but this keeps local runs and CI self-contained.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic chatTopic(@Value("${app.kafka.topic}") String topic,
                       @Value("${app.kafka.partitions}") int partitions,
                       @Value("${app.kafka.replicas}") short replicas) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }
}
