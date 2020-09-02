package io.confluent.developer.ccloud.demo.kstreams.topic

import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.PropertySource

@Configuration
@PropertySource(value = ["classpath:topics-defaults.properties"])
abstract class TopicConfig {
  var name: String = ""
  var compacted: Boolean = false
  var partitions: Int = 1
  var replicationFactor: Int = 1
}