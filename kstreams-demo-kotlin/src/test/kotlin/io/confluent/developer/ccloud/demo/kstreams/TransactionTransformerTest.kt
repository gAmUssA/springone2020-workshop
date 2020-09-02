package io.confluent.developer.ccloud.demo.kstreams

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.developer.ccloud.demo.kstreams.domain.Funds
import io.confluent.developer.ccloud.demo.kstreams.domain.Transaction
import io.confluent.developer.ccloud.demo.kstreams.domain.TransactionResult
import io.confluent.developer.ccloud.demo.kstreams.domain.TransactionResult.ErrorType.INSUFFICIENT_FUNDS
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.MockProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerde
import java.math.BigDecimal
import java.util.*

class TransactionTransformerTest {
  private var fundsStore: KeyValueStore<String, Funds>? = null
  private var mockContext: MockProcessorContext? = null
  private var transactionTransformer: TransactionTransformer? = null

  @BeforeEach
  fun setup() {
    val properties = Properties()

    properties.putAll(testConfig)
    mockContext = MockProcessorContext(properties)
    fundsStore = Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("fundsStore"),
        Serdes.String(),
        JsonSerde<Funds>(Funds::class.java, OBJECT_MAPPER))
        .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
        .build()
    fundsStore?.init(mockContext, fundsStore)
    mockContext!!.register(fundsStore, null)
    transactionTransformer = TransactionTransformer(fundsStore?.name()!!)
    transactionTransformer!!.init(mockContext!!)
  }

  @Test
  fun shouldStoreTransaction() {
    val transaction = Transaction(UUID.randomUUID().toString(),
        "1",
        BigDecimal(100),
        Transaction.Type.DEPOSIT,
        "USD",
        "USA")
    val transactionResult: TransactionResult = transactionTransformer?.transform(transaction)!!

    assertThat(transactionResult.success, `is`(true))
  }

  @Test
  fun shouldHaveInsufficientFunds() {
    val transaction = Transaction(UUID.randomUUID().toString(), "1", BigDecimal("100"), Transaction.Type.WITHDRAW, "RUR",
        "Russia")
    val result: TransactionResult = transactionTransformer?.transform(transaction)!!
    assertThat<Boolean?>(result.success, `is`(false))
    assertThat(result.errorType, `is`(INSUFFICIENT_FUNDS))
  }

  @Test
  fun shouldHaveEnoughFunds() {
    val transaction1 = Transaction(UUID.randomUUID().toString(), "1", BigDecimal("300"), Transaction.Type.DEPOSIT, "RUR",
        "Russia")
    val transaction2 = Transaction(UUID.randomUUID().toString(), "1", BigDecimal("200"), Transaction.Type.WITHDRAW, "RUR",
        "Russia")
    transactionTransformer?.transform(transaction1)
    val result: TransactionResult = transactionTransformer?.transform(transaction2)!!
    assertThat(result.success, `is`(true))
    assertThat(result.errorType, `is`(nullValue()))
  }

  companion object {
    private val OBJECT_MAPPER = ObjectMapper().findAndRegisterModules()
    val testConfig = mapOf(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:8080",
        StreamsConfig.APPLICATION_ID_CONFIG to "mytest",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to "org.apache.kafka.common.serialization.Serdes\$StringSerde",
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to "org.springframework.kafka.support.serializer.JsonSerde",
        JsonDeserializer.TYPE_MAPPINGS to "transaction:io.confluent.developer.ccloud.demo.kstreams.domain.Transaction",
        JsonDeserializer.TRUSTED_PACKAGES to "*"
    )
  }
}