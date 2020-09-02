package io.confluent.developer.ccloud.demo.kstreams.topic

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties("stores.funds")
data class FundsStoreConfig(var name: String = "funds") 