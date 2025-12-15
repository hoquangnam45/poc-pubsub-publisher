package com.cdc.poc.pubsub.publisher.repo;

import com.cdc.poc.pubsub.publisher.model.PendingStressTestConfiguration;
import com.cdc.poc.pubsub.publisher.model.PublishResult;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface StressTestRepo {
    void createStressTestConfiguration(PendingStressTestConfiguration configuration);
    void createPublishResult(PublishResult result);
}
