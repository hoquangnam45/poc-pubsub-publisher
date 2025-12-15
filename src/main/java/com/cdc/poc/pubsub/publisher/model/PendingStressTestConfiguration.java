package com.cdc.poc.pubsub.publisher.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PendingStressTestConfiguration(
        UUID testId,
        Integer numOfMessage,
        BigDecimal minMessageSizeInKb,
        BigDecimal maxMessageSizeInKb,
        BigDecimal rangeStdDev,
        String description,
        Instant startTime
) {
}
