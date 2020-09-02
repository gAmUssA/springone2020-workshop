package io.confluent.developer.ccloud.demo.kstreams.topic

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("topics.transaction-request")
class TransactionRequestTopicConfig : TopicConfig()