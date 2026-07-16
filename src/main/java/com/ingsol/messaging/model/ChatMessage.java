package com.ingsol.messaging.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * The single message contract shared across REST, WebSocket, and Kafka.
 * A record because it is immutable data with no behaviour: no boilerplate, no Lombok.
 *
 * roomId doubles as the Kafka partition key so all messages for one room land on
 * one partition and therefore keep their order. Ordering across different rooms
 * does not matter, so we do not force everything onto a single partition.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ChatMessage(

        @NotBlank @Size(max = 64)
        String roomId,

        @NotBlank @Size(max = 64)
        String sender,

        @NotBlank @Size(max = 4000)
        String content,

        Instant sentAt
) {
    /** Server stamps the timestamp; clients do not get to lie about it. */
    public ChatMessage withServerTimestamp(Instant now) {
        return new ChatMessage(roomId, sender, content, now);
    }
}
