package com.cdc.poc.pubsub.publisher.repo;

import com.cdc.poc.pubsub.publisher.model.PendingStressTestConfigurationMdl;
import com.cdc.poc.pubsub.publisher.model.PublishResult;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface StressTestRepo {
    void createStressTestConfiguration(PendingStressTestConfigurationMdl configuration);
    void createPublishResult(PublishResult result);
}
