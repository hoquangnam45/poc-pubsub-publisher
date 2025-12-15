CREATE TABLE IF NOT EXISTS test_configuration (
    test_id UUID PRIMARY KEY,
    num_of_message INTEGER,
    min_message_size_in_kb DECIMAL,
    max_message_size_in_kb DECIMAL,
    range_std_dev DECIMAL,
    description VARCHAR(255),
    start_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS test_publish_result (
    test_id UUID,
    message_id UUID,
    success BOOLEAN,
    error_message TEXT,
    message_size_in_kb DECIMAL,
    serialized_size_in_kb DECIMAL,
    topic_id VARCHAR(255),
    created_at TIMESTAMP,
    PRIMARY KEY (test_id, message_id),
    FOREIGN KEY (test_id) REFERENCES test_configuration(test_id)
);

CREATE TABLE IF NOT EXISTS test_topic_result (
    test_id UUID,
    message_id UUID,
    topic_publish_time TIMESTAMP,
    topic_arrival_time TIMESTAMP,
    PRIMARY KEY (test_id, message_id),
    FOREIGN KEY (test_id) REFERENCES test_configuration(test_id)
);

CREATE TABLE IF NOT EXISTS test_subscriber_result
(
    test_id                   UUID,
    message_id                UUID,
    subscription_publish_time TIMESTAMP,
    subscription_arrival_time TIMESTAMP,
    subscriber_arrival_time   TIMESTAMP,
    subscription_type VARCHAR(255),
    subscription_id VARCHAR(255),
    pull_options VARCHAR(255),
    created_at                TIMESTAMP,
    PRIMARY KEY (test_id, message_id, subscription_id, pull_options)
--     FOREIGN KEY (test_id, message_id) REFERENCES test_topic_result (test_id, message_id)
);

-- CREATE TABLE IF NOT EXISTS polling_subscriber_result(
--     test_id UUID,
--     message_id UUID,
--     polling_start_time TIMESTAMP,
--     message_receive_time TIMESTAMP,
--     PRIMARY KEY (test_id, message_id),
--     FOREIGN KEY (test_id, message_id) REFERENCES test_result (test_id, message_id)
-- )
