package com.cdc.poc.pubsub.publisher.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record PendingStressTestConfigurationMdl(
        UUID testId,
        Integer numOfMessage,
        BigDecimal minMessageSizeInKb,
        BigDecimal maxMessageSizeInKb,
        BigDecimal rangeStdDev,
        String description,
        String includedTopics,
        Instant startTime
) {
}
