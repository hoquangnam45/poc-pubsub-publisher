package com.cdc.poc.pubsub.publisher.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Message(UUID testId, UUID messageId, String payload, Instant creationTimeStamp, BigDecimal payloadSizeInKb) {
}
