package com.messaging.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * STOMP over WebSocket.
 *
 * - Clients connect to /ws (SockJS fallback for browsers/proxies that block raw WS).
 * - Clients SEND to destinations prefixed /app -> routed to @MessageMapping methods.
 * - Clients SUBSCRIBE to /topic/** -> served by the in-memory simple broker.
 *
 * The simple broker only knows about clients connected to THIS instance. Cross-instance
 * delivery is Kafka's job, not the broker's. See ChatConsumer.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                // ponytail: wide-open origins for local dev. Lock to real origins in prod.
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }
}
