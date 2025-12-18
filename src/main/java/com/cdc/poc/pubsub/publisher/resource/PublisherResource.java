package com.cdc.poc.pubsub.publisher.resource;

import com.cdc.poc.pubsub.publisher.model.Message;
import com.cdc.poc.pubsub.publisher.model.PendingStressTestConfiguration;
import com.cdc.poc.pubsub.publisher.model.PendingStressTestConfigurationMdl;
import com.cdc.poc.pubsub.publisher.model.PublishResult;
import com.cdc.poc.pubsub.publisher.model.StressTestConfiguration;
import com.cdc.poc.pubsub.publisher.repo.StressTestRepo;
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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Path("/publish")
public class PublisherResource {
    private static final Random random = new Random();
    private static final LinkedBlockingQueue<PendingStressTestConfiguration> stressTestQueue = new LinkedBlockingQueue<>();
    private static final LinkedBlockingQueue<PublishResult> PUBLISH_RESULT_QUEUE = new LinkedBlockingQueue<>();
    private static final Map<String, Publisher> PUBLISHER_MAP = new ConcurrentHashMap<>();

    @Inject
    QuarkusPubSub pubSub;

    @Inject
    StressTestRepo stressTestRepo;

    @ConfigProperty(name = "pubsub.topics")
    List<String> topics;

    @ConfigProperty(name = "workers.topic-result.size", defaultValue = "10")
    Integer workersTopicResultSize;

    @Startup
    public void initSendMessage() throws IOException {
        for (String topicName : topics) {
            log.info("Initializing publisher for topic: {}", topicName);
            PUBLISHER_MAP.put(topicName, pubSub.publisher(topicName));
        }
        log.info("Publisher initialized successfully. Starting stress test executor thread.");
        ExecutorService stressTestExecutor = Executors.newFixedThreadPool(1);
        ExecutorService messageSendingExecutor = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService topicResultExecutor = Executors.newFixedThreadPool(workersTopicResultSize);
        stressTestExecutor.submit(() -> {
            while (true) {
                PendingStressTestConfiguration configuration = stressTestQueue.take();
                log.info(
                        "Starting stress test: testId={}, numOfMessages={}, minSizeKb={}, maxSizeKb={}, rangeStdDev={}, description={}, queueDepth={}",
                        configuration.testId(), configuration.numOfMessage(), configuration.minMessageSizeInKb(),
                        configuration.maxMessageSizeInKb(), configuration.rangeStdDev(), configuration.description(), stressTestQueue.size());
                Instant testStartTime = Instant.now();
                AtomicInteger completedCount = new AtomicInteger(0);
                int totalMessages = configuration.numOfMessage();
                for (int i = 0; i < configuration.numOfMessage(); i++) {
                    Instant messageGenStartTime = Instant.now();
                    BigDecimal sizeOfMessageInKb = generateBoundedNormal(configuration.minMessageSizeInKb(),
                            configuration.maxMessageSizeInKb(), configuration.rangeStdDev());
                    Message message = generateMessage(configuration.testId(), sizeOfMessageInKb);
                    long messageGenDurationMs = Duration.between(messageGenStartTime, Instant.now()).toMillis();
                    if (i % 100 == 0) {
                        log.debug("Generated message {}/{}[description={}] for testId={}, messageId={}, sizeKb={}, generationTimeMs={}",
                                i + 1, configuration.numOfMessage(), configuration.description(), message.testId(), message.messageId(),
                                message.payloadSizeInKb(), messageGenDurationMs);
                    }
                    PubsubMessage.Builder pubsubMessageBuilder = PubsubMessage.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFromUtf8(message.payload()))
                            .putAttributes("testId", message.testId().toString())
                            .putAttributes("messageId", message.messageId().toString())
                            .putAttributes("payloadSizeInKb", message.payloadSizeInKb().toPlainString())
                            .putAttributes("creationTimeStamp", message.creationTimeStamp().toString());
                    final int messageIndex = i;
                    List<String> includedTopics = configuration.includedTopics() == null || configuration.includedTopics().isEmpty() ? topics : configuration.includedTopics();
                    for (String topicName : includedTopics) {
                        PubsubMessage pubsubMessage = pubsubMessageBuilder
                                .putAttributes("topic_id", topicName)
                                .build();
                        messageSendingExecutor.submit(() -> {
                            Instant publishStartTime = Instant.now();
                            try {
                                // NOTE: should be auto-mount on virtual thread
                                PUBLISHER_MAP.get(topicName).publish(pubsubMessage).get();
                                long publishDurationMs = Duration.between(publishStartTime, Instant.now()).toMillis();
                                BigDecimal serializedSizeKb = divide(BigDecimal.valueOf(pubsubMessage.getSerializedSize()),
                                        BigDecimal.valueOf(1024));
                                if (messageIndex % 100 == 0) {
                                    log.info(
                                            "Published message {}/{}[description={}]: testId={}, messageId={}, payloadSizeKb={}, serializedSizeKb={}, publishLatencyMs={}",
                                            messageIndex + 1, configuration.numOfMessage(), configuration.description(), message.testId(),
                                            message.messageId(), message.payloadSizeInKb(), serializedSizeKb,
                                            publishDurationMs);
                                }
                                PUBLISH_RESULT_QUEUE.add(new PublishResult(message.testId(), message.messageId(),
                                        true, null, message.payloadSizeInKb(), serializedSizeKb, topicName, message.creationTimeStamp(), publishStartTime));
                            } catch (Exception e) {
                                long publishDurationMs = Duration.between(publishStartTime, Instant.now()).toMillis();
                                log.error(
                                        "Failed to publish message: testId={}, messageId={}, payloadSizeInKb={}, serializedSizeKb={}, publishLatencyMs={}, description={}, creationTimestamp={}, error={}",
                                        message.testId(), message.messageId(), message.payloadSizeInKb(),
                                        divide(BigDecimal.valueOf(pubsubMessage.getSerializedSize()),
                                                BigDecimal.valueOf(1024)),
                                        publishDurationMs, configuration.description(), message.creationTimeStamp(), e.getMessage(), e);
                                PUBLISH_RESULT_QUEUE.add(new PublishResult(message.testId(), message.messageId(),
                                        false, e.getMessage(), message.payloadSizeInKb(),
                                        divide(BigDecimal.valueOf(pubsubMessage.getSerializedSize()),
                                                BigDecimal.valueOf(1024)),
                                        topicName, message.creationTimeStamp(), publishStartTime));
                            } finally {
                                int completed = completedCount.incrementAndGet();
                                if (completed == totalMessages) {
                                    long totalPublishTimeMs = Duration.between(testStartTime, Instant.now()).toMillis();
                                    log.info(
                                            "All messages published for testId={}, totalMessages={}, totalPublishTimeMs={}, avgPublishTimePerMessageMs={}, description={}",
                                            configuration.testId(), totalMessages, totalPublishTimeMs,
                                            totalPublishTimeMs / totalMessages, configuration.description());
                                }
                            }
                            return true;
                        });
                    }
                }
            }
        });
        topicResultExecutor.submit(() -> {
            while (true) {
                PublishResult result = PUBLISH_RESULT_QUEUE.take();
                try {
                    stressTestRepo.createPublishResult(result);
                } catch (Exception e) {
                    log.error("Failed to persist topic result message: testId={}, messageId={}, payloadSizeInKb={}, serializedSizeKb={}, creationTimestamp={}, error={}", result.testId(), result.messageId(), result.messageSizeInKb(), result.serializedSizeInKb(), result.createdAt(), e.getMessage(), e);
                }
            }
        });
    }

    @POST
    public Uni<UUID> stressTestTrigger(StressTestConfiguration configuration) {
        log.info("Received stress test trigger request: {}", configuration);
        int numberOfMessage = configuration.numOfMessage() == null || configuration.numOfMessage() <= 0 ? 1
                : configuration.numOfMessage();
        BigDecimal minMessageSizeInKb = configuration.minMessageSizeInKb() == null ? BigDecimal.ONE
                : configuration.minMessageSizeInKb().compareTo(BigDecimal.ONE) < 0 ? BigDecimal.ONE
                        : configuration.minMessageSizeInKb();
        BigDecimal maxMessageSizeInKb = configuration.maxMessageSizeInKb() == null ? BigDecimal.ONE
                : minMessageSizeInKb
                        .min(configuration.maxMessageSizeInKb().compareTo(BigDecimal.ONE) < 0 ? BigDecimal.ONE
                                : configuration.maxMessageSizeInKb());
        BigDecimal rangeStdDev = configuration.rangeStdDev() == null
                || configuration.rangeStdDev().compareTo(BigDecimal.ZERO) <= 0 ? BigDecimal.valueOf(6)
                        : configuration.rangeStdDev(); // The range between (min, max) should cover how much standard
                                                       // deviation
        UUID testId = UUID.randomUUID();
        Instant startTestTime = Instant.now();
        PendingStressTestConfiguration pendingConfiguration = new PendingStressTestConfiguration(testId,
                numberOfMessage, minMessageSizeInKb, maxMessageSizeInKb, rangeStdDev, configuration.description(), configuration.includedTopics(), startTestTime);
        log.info(
                "Create stress test configuration: testId={}, numberOfMessages={}, minSizeKb={}, maxSizeKb={}, rangeStdDev={}, startTime={}, description={}, currentQueueDepth={}",
                testId, numberOfMessage, minMessageSizeInKb, maxMessageSizeInKb, rangeStdDev, startTestTime, configuration.description(),
                stressTestQueue.size());
        PendingStressTestConfigurationMdl mdl = new PendingStressTestConfigurationMdl(pendingConfiguration.testId(), pendingConfiguration.numOfMessage(), pendingConfiguration.minMessageSizeInKb(), pendingConfiguration.maxMessageSizeInKb(), pendingConfiguration.rangeStdDev(), pendingConfiguration.description(), pendingConfiguration.includedTopics() == null ? "" : String.join(",", pendingConfiguration.includedTopics()), pendingConfiguration.startTime());
        stressTestRepo.createStressTestConfiguration(mdl);
        stressTestQueue.add(pendingConfiguration);
        log.info("Stress test queued successfully: testId={}, newQueueDepth={}", testId, stressTestQueue.size());
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
