package com.ingsol.messaging.web;

import com.ingsol.messaging.messaging.ChatProducer;
import com.ingsol.messaging.model.ChatMessage;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST entry point for non-browser or server-to-server senders (bots, integrations,
 * mobile clients that prefer HTTP). Same path into Kafka as the WebSocket controller.
 */
@RestController
@RequestMapping("/api/messages")
public class ChatRestController {

    private final ChatProducer producer;

    public ChatRestController(ChatProducer producer) {
        this.producer = producer;
    }

    /**
     * Publish a message. 202 Accepted, not 201 Created: we have handed it to Kafka
     * for asynchronous fan-out, we are not returning a stored, addressable resource.
     */
    @PostMapping
    public ResponseEntity<ChatMessage> send(@Valid @RequestBody ChatMessage message) {
        ChatMessage accepted = producer.publish(message);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(accepted);
    }
}
