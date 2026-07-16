package com.ingsol.messaging.web;

import com.ingsol.messaging.messaging.ChatProducer;
import com.ingsol.messaging.model.ChatMessage;
import jakarta.validation.Valid;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

/**
 * WebSocket entry point. A client SENDs to /app/chat.send.
 *
 * Note it does NOT broadcast directly. It hands the message to Kafka and returns.
 * Delivery back to subscribers happens via ChatConsumer, so a client connected to
 * any instance receives it. Broadcasting here instead would only reach this instance.
 */
@Controller
public class ChatWebSocketController {

    private final ChatProducer producer;

    public ChatWebSocketController(ChatProducer producer) {
        this.producer = producer;
    }

    @MessageMapping("chat.send")
    public void send(@Payload @Valid ChatMessage message) {
        producer.publish(message);
    }
}
