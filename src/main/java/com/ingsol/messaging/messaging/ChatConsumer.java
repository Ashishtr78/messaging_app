package com.ingsol.messaging.messaging;

import com.ingsol.messaging.model.ChatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumes every chat message off Kafka and pushes it to the WebSocket clients
 * connected to THIS instance.
 *
 * Why unique consumer group per instance (see application.yml group-id with random.uuid):
 * a chat message must reach every client no matter which instance holds their socket.
 * If all instances shared one group, Kafka would give each message to only one instance,
 * and clients on the other instances would never see it. A unique group per instance means
 * every instance receives every message and fans it out to its own local subscribers.
 */
@Component
public class ChatConsumer {

    private static final Logger log = LoggerFactory.getLogger(ChatConsumer.class);

    private final SimpMessagingTemplate broker;

    public ChatConsumer(SimpMessagingTemplate broker) {
        this.broker = broker;
    }

    @KafkaListener(topics = "${app.kafka.topic}")
    public void onMessage(ChatMessage message) {
        String destination = "/topic/rooms/" + message.roomId();
        log.debug("Delivering message from {} to {}", message.sender(), destination);
        broker.convertAndSend(destination, message);
    }
}
