package com.cdc.poc.pubsub.publisher.resource;

import com.cdc.poc.pubsub.publisher.model.Message;
import com.cdc.poc.pubsub.publisher.model.PendingStressTestConfiguration;
import com.cdc.poc.pubsub.publisher.model.QueueResult;
import com.cdc.poc.pubsub.publisher.model.StressTestConfiguration;
import com.cdc.poc.pubsub.publisher.repo.StressTestRepo;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import io.quarkiverse.googlecloudservices.pubsub.QuarkusPubSub;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Path("/publish")
public class PublisherResource {
    private static final Random random = new Random();
    private static final LinkedBlockingQueue<PendingStressTestConfiguration> stressTestQueue = new LinkedBlockingQueue<>();

    @Inject
    QuarkusPubSub pubSub;

    @Inject
    StressTestRepo stressTestRepo;

    @ConfigProperty(name = "pubsub.topic")
    String topicName;

    @Startup
    public void initSendMessage() throws IOException {
        ExecutorService stressTestExecutor = Executors.newFixedThreadPool(1);
        ExecutorService messageSendingExecutor = Executors.newVirtualThreadPerTaskExecutor();
        Publisher publisher = pubSub.publisher(topicName);
        stressTestExecutor.submit(() -> {
            while (true) {
                PendingStressTestConfiguration configuration = stressTestQueue.take();
                for (int i = 0; i < configuration.numOfMessage(); i++) {
                    BigDecimal sizeOfMessageInKb = generateBoundedNormal(configuration.minMessageSizeInKb(), configuration.maxMessageSizeInKb(), configuration.rangeStdDev());
                    Message message = generateMessage(configuration.testId(), sizeOfMessageInKb);
                    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFromUtf8(message.payload()))
                            .putAttributes("testId", message.testId().toString())
                            .putAttributes("messageId", message.messageId().toString())
                            .putAttributes("payloadSizeInKb", message.payloadSizeInKb().toPlainString())
                            .putAttributes("creationTimeStamp", message.creationTimeStamp().toString())
                            .build();
                    messageSendingExecutor.submit(() -> {
                        try {
                            publisher.publish(pubsubMessage).get();
                            stressTestRepo.createQueueResult(new QueueResult(message.testId(), message.messageId(), true, null, message.payloadSizeInKb(), divide(BigDecimal.valueOf(pubsubMessage.getSerializedSize()), BigDecimal.valueOf(1024)), topicName, Instant.now()));
                        } catch (Exception e) {
                            log.error("Failed to publish message: testId = {}, messageId = {}, payloadSizeInKb = {}, creationTimestamp = {}", message.testId(), message.messageId(), message.payloadSizeInKb(), message.creationTimeStamp(), e);
                            stressTestRepo.createQueueResult(new QueueResult(message.testId(), message.messageId(), false, e.getMessage(), message.payloadSizeInKb(), divide(BigDecimal.valueOf(pubsubMessage.getSerializedSize()), BigDecimal.valueOf(1024)), topicName, Instant.now()));
                        }
                        return true;
                    });
                }
            }
        });
    }

    @POST
    public Uni<UUID> stressTestTrigger(StressTestConfiguration configuration) {
        int numberOfMessage = configuration.numOfMessage() == null || configuration.numOfMessage() <= 0 ? 1 : configuration.numOfMessage();
        BigDecimal minMessageSizeInKb = configuration.minMessageSizeInKb() == null ? BigDecimal.ONE : configuration.minMessageSizeInKb().compareTo(BigDecimal.ONE) < 0 ? BigDecimal.ONE : configuration.minMessageSizeInKb();
        BigDecimal maxMessageSizeInKb = configuration.maxMessageSizeInKb() == null ? BigDecimal.ONE : minMessageSizeInKb.min(configuration.maxMessageSizeInKb().compareTo(BigDecimal.ONE) < 0 ? BigDecimal.ONE : configuration.maxMessageSizeInKb());
        BigDecimal rangeStdDev = configuration.rangeStdDev() == null || configuration.rangeStdDev().compareTo(BigDecimal.ZERO) <= 0 ? BigDecimal.valueOf(6) : configuration.rangeStdDev(); // The range between (min, max) should cover how much standard deviation
        UUID testId = UUID.randomUUID();
        Instant startTestTime = Instant.now();
        PendingStressTestConfiguration pendingConfiguration = new PendingStressTestConfiguration(testId, numberOfMessage, minMessageSizeInKb, maxMessageSizeInKb, rangeStdDev, startTestTime);
        stressTestRepo.createStressTestConfiguration(pendingConfiguration);
        stressTestQueue.add(pendingConfiguration);
        return Uni.createFrom().item(testId);
    }

    private static BigDecimal generateBoundedNormal(BigDecimal min, BigDecimal max, BigDecimal rangeStdDev) {
        if (min.compareTo(max) == 0) {
            return min;
        }
        BigDecimal mean = divide(min.add(max), BigDecimal.TWO);
        BigDecimal range = max.subtract(min);
        BigDecimal rangeStdUnit = divide(range, rangeStdDev);
        BigDecimal randomStdStep = BigDecimal.valueOf(random.nextGaussian());
        BigDecimal scaleAndShift;
        do {
            scaleAndShift = randomStdStep.multiply(rangeStdUnit).add(mean);
        } while (!(scaleAndShift.compareTo(min) >= 0 && scaleAndShift.compareTo(max) <= 0));
        return scaleAndShift;
    }

    private static BigDecimal divide(BigDecimal num, BigDecimal divisor) {
        return num.divide(divisor, 12, RoundingMode.HALF_EVEN);
    }

    private static Message generateMessage(UUID testId, BigDecimal sizeInKb) {
        UUID messageId = UUID.randomUUID();
        String payload = generateRandomStringWithSize(sizeInKb);
        return new Message(testId, messageId, payload, Instant.now(), sizeInKb);
    }

    private static String generateRandomStringWithSize(BigDecimal sizeInKb) {
        // 1. Convert KB to Bytes (1 KB = 1024 Bytes)
        // We use longValue() because array sizes must be integers.
        long sizeInBytes = sizeInKb.multiply(new BigDecimal("1024")).longValue();

        // Safety check for empty or negative sizes
        if (sizeInBytes <= 0) {
            return "";
        }

        // Safety check for Java Array limit (approx 2GB)
        if (sizeInBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("String size exceeds Java heap/array limits.");
        }

        // 2. Define the characters to choose from (Alphanumeric)
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder((int) sizeInBytes);
        Random random = new Random();

        // 3. Generate the string
        for (int i = 0; i < sizeInBytes; i++) {
            int index = random.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }

        return sb.toString();
    }
}
