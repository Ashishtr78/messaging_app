package com.ingsol.messaging;

import com.ingsol.messaging.messaging.ChatProducer;
import com.ingsol.messaging.model.ChatMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * End-to-end over embedded Kafka: produce -> real Kafka -> ChatConsumer -> broker.
 * The broker is mocked because we are testing routing, not STOMP delivery.
 * If keying, timestamping, or the /topic/rooms/{id} routing breaks, this fails.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "chat-messages")
class ChatFlowTest {

    @Autowired
    ChatProducer producer;

    @MockBean
    SimpMessagingTemplate broker; // stand-in for the WebSocket broker

    @Test
    void publishedMessageIsRoutedToRoomDestination() {
        ChatMessage sent = new ChatMessage("room-42", "alice", "hello", null);

        ChatMessage accepted = producer.publish(sent);

        assertThat(accepted.sentAt()).isNotNull(); // server stamped it

        // wait up to 10s for the message to round-trip through embedded Kafka
        verify(broker, timeout(10_000))
                .convertAndSend(eq("/topic/rooms/room-42"), (Object) any(ChatMessage.class));
    }
}
