package com.cdc.poc.pubsub.publisher.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TopicResult(UUID testId, UUID messageId, boolean success, String errorMessage, BigDecimal messageSizeInKb, BigDecimal serializedSizeInKb, String topicId, Instant createdAt) {
}
