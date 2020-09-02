package io.confluent.developer.ccloud.demo.kstreams

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.developer.ccloud.demo.kstreams.domain.Funds
import io.confluent.developer.ccloud.demo.kstreams.domain.Transaction
import io.confluent.developer.ccloud.demo.kstreams.domain.TransactionResult
import io.confluent.developer.ccloud.demo.kstreams.topic.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.ValueTransformerSupplier
import org.apache.kafka.streams.state.Stores
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.support.serializer.JsonSerde

@Configuration
@EnableKafkaStreams
class KStreamConfig(
    private val transactionRequestConfiguration: TransactionRequestTopicConfig,
    private val transactionSuccessConfiguration: TransactionSuccessTopicConfig,
    private val transactionFailedConfiguration: TransactionFailedTopicConfig,
    private val fundsStoreConfig: FundsStoreConfig) {

  val log = logger<KStreamConfig>()

  @Bean
  fun transactionFailed(topicConfig: TransactionFailedTopicConfig): NewTopic {
    return createTopic(topicConfig)
  }

  @Bean
  fun transactionSuccess(topicConfig: TransactionSuccessTopicConfig): NewTopic {
    return createTopic(topicConfig)
  }

  private fun createTopic(topicConfig: TopicConfig): NewTopic {
    log.info("Creating topic {}...", topicConfig.name)
    return TopicBuilder.name(topicConfig.name)
        .partitions(topicConfig.partitions)
        .replicas(topicConfig.partitions)
        .compact()
        .build()
  }

  @Bean
  fun topology(streamsBuilder: StreamsBuilder): Topology {
    streamsBuilder.addStateStore(
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(fundsStoreConfig.name),
            Serdes.String(), JsonSerde<Funds>(Funds::class.java, OBJECT_MAPPER)))
    defineStreams(streamsBuilder)
    val topology = streamsBuilder.build()
    log.trace("Topology description : {}", topology.describe())
    return topology
  }

  protected fun defineStreams(streamsBuilder: StreamsBuilder) {
    val storeName: String = fundsStoreConfig.name

    val transactionStream: KStream<String, Transaction> = streamsBuilder.stream(transactionRequestConfiguration.name)

    val resultStream: KStream<String, TransactionResult> = transactionStream
        .transformValues(ValueTransformerSupplier { TransactionTransformer(storeName) }, storeName)

    resultStream
        .filter { account: String, result: TransactionResult -> success(account, result) }
        .to(transactionSuccessConfiguration.name)

    resultStream
        .filterNot { account: String, result: TransactionResult -> success(account, result) }
        .to(transactionFailedConfiguration.name)
  }

  private fun success(account: String, result: TransactionResult): Boolean {
    return result.success
  }

  companion object {
    private val OBJECT_MAPPER = ObjectMapper().findAndRegisterModules()
  }
}