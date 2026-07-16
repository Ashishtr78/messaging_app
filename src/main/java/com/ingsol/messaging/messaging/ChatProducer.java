package com.ingsol.messaging.messaging;

import com.ingsol.messaging.model.ChatMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * The one place that writes to Kafka. Both the REST and WebSocket entry points call this,
 * so there is a single, testable path for "a message was accepted".
 */
@Service
public class ChatProducer {

    private final KafkaTemplate<String, ChatMessage> kafka;
    private final String topic;

    public ChatProducer(KafkaTemplate<String, ChatMessage> kafka,
                        @Value("${app.kafka.topic}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    /**
     * Accept a message: stamp server time, then publish keyed by roomId.
     * Keying by roomId guarantees per-room ordering (same key -> same partition).
     */
    public ChatMessage publish(ChatMessage message) {
        ChatMessage stamped = message.withServerTimestamp(Instant.now());
        kafka.send(topic, stamped.roomId(), stamped);
        return stamped;
    }
}
