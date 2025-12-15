package com.cdc.poc.pubsub.publisher.model;

import java.math.BigDecimal;

public record StressTestConfiguration(
        Integer numOfMessage,
        BigDecimal minMessageSizeInKb,
        BigDecimal maxMessageSizeInKb,
        BigDecimal rangeStdDev,
        String description
) {
}
