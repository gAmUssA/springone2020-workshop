package io.confluent.developer.ccloud.demo.kstreams

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.developer.ccloud.demo.kstreams.domain.Funds
import io.confluent.developer.ccloud.demo.kstreams.domain.Transaction
import io.confluent.developer.ccloud.demo.kstreams.domain.TransactionResult
import io.confluent.developer.ccloud.demo.kstreams.topic.FundsStoreConfig
import io.confluent.developer.ccloud.demo.kstreams.topic.TransactionFailedTopicConfig
import io.confluent.developer.ccloud.demo.kstreams.topic.TransactionRequestTopicConfig
import io.confluent.developer.ccloud.demo.kstreams.topic.TransactionSuccessTopicConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.state.KeyValueStore
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde
import java.math.BigDecimal
import java.util.*

class KStreamConfigTest {
  var txRequestConfig: TransactionRequestTopicConfig? = null
  var txSuccessConfig: TransactionSuccessTopicConfig? = null
  var txFailedTopicConfig: TransactionFailedTopicConfig? = null

  private lateinit var transactionSerde: JsonSerde<Transaction>
  private lateinit var transactionResultSerde: JsonSerde<TransactionResult>
  private lateinit var stringSerde: Serde<String>

  private lateinit var deposit100: Transaction
  private lateinit var withdraw100: Transaction
  private lateinit var withdraw200: Transaction

  private var fundsStoreConfig: FundsStoreConfig? = null
  private lateinit var properties: Properties
  private lateinit var topology: Topology

  @BeforeEach
  fun setUp() {

    // TODO use spring test and test configs
    txRequestConfig = TransactionRequestTopicConfig()
    txRequestConfig!!.name = "transaction-request"
    txSuccessConfig = TransactionSuccessTopicConfig()
    txSuccessConfig!!.name = "transaction-success"
    txFailedTopicConfig = TransactionFailedTopicConfig()
    txFailedTopicConfig!!.name = "transaction-failed"
    fundsStoreConfig = FundsStoreConfig()
    fundsStoreConfig!!.name = "funds-store"
    val streamsBuilder = StreamsBuilder()

    topology = KStreamConfig(txRequestConfig!!, txSuccessConfig!!, txFailedTopicConfig!!, fundsStoreConfig!!)
        .topology(streamsBuilder)

    // serdes
    transactionSerde = JsonSerde(Transaction::class.java, OBJECT_MAPPER)
    transactionResultSerde = JsonSerde(TransactionResult::class.java, OBJECT_MAPPER)
    stringSerde = Serdes.String()

    // ttd
    properties = Properties()
    properties.putAll(TransactionTransformerTest.testConfig)
    deposit100 = Transaction(UUID.randomUUID().toString(),
        "1",
        BigDecimal(100),
        Transaction.Type.DEPOSIT,
        "USD",
        "USA")
    withdraw100 = Transaction(UUID.randomUUID().toString(),
        "1",
        BigDecimal(100),
        Transaction.Type.WITHDRAW,
        "USD",
        "USA")
    withdraw200 = Transaction(UUID.randomUUID().toString(),
        "1",
        BigDecimal(200),
        Transaction.Type.WITHDRAW,
        "USD",
        "USA")
  }

  @Test
  fun testDriverShouldNotBeNull() {
    TopologyTestDriver(topology, properties).use { testDriver ->
      assertThat(testDriver, not(nullValue())
      )
    }
  }

  @Test
  fun shouldCreateSuccessfulTransaction() {
    TopologyTestDriver(topology, properties).use { testDriver ->
      val inputTopic: TestInputTopic<String, Transaction> = testDriver
          .createInputTopic(txRequestConfig?.name, stringSerde.serializer(), transactionSerde.serializer())
      inputTopic.pipeInput(deposit100.account, deposit100)
      inputTopic.pipeInput(withdraw100.account, withdraw100)
      val outputTopic: TestOutputTopic<String, TransactionResult> =
          testDriver.createOutputTopic(txSuccessConfig?.name,
              stringSerde.deserializer(),
              transactionResultSerde.deserializer())
      val successfulTransactions: List<TransactionResult> = outputTopic.readValuesToList()
      // balance should be 0
      val transactionResult: TransactionResult = successfulTransactions[1]
      assertThat(transactionResult.funds.balance, `is`(BigDecimal(0)))
    }
  }

  @Test
  fun shouldBeInsufficientFunds() {
    TopologyTestDriver(topology, properties).use { testDriver ->
      val inputTopic: TestInputTopic<String, Transaction> = testDriver
          .createInputTopic(txRequestConfig?.name, stringSerde.serializer(), transactionSerde.serializer())
      inputTopic.pipeInput(deposit100.account, deposit100)
      inputTopic.pipeInput(withdraw200.account, withdraw200)
      val failedResultOutputTopic: TestOutputTopic<String, TransactionResult> = testDriver.createOutputTopic(txFailedTopicConfig?.name, stringSerde.deserializer(),
          transactionResultSerde.deserializer())
      val successResultOutputTopic: TestOutputTopic<String, TransactionResult> = testDriver.createOutputTopic(txSuccessConfig?.name, stringSerde.deserializer(),
          transactionResultSerde.deserializer())
      val successfulDeposit100Result: TransactionResult = successResultOutputTopic.readValuesToList()[0]
      assertThat(successfulDeposit100Result.funds.balance, `is`(BigDecimal(100)))

      val failedTransactions: List<TransactionResult> = failedResultOutputTopic.readValuesToList()
      // balance should be 0
      val transactionResult: TransactionResult = failedTransactions[0]
      assertThat(transactionResult.errorType, `is`(TransactionResult.ErrorType.INSUFFICIENT_FUNDS))
    }
  }

  @Test
  fun balanceShouldBe300() {
    TopologyTestDriver(topology, properties).use { testDriver ->
      val inputTopic: TestInputTopic<String, Transaction?> =
          testDriver.createInputTopic(txRequestConfig?.name, stringSerde.serializer(), transactionSerde.serializer())
      inputTopic.pipeInput(deposit100.account, deposit100)
      inputTopic.pipeInput(deposit100.account, deposit100)
      inputTopic.pipeInput(deposit100.account, deposit100)
      val store: KeyValueStore<String, Funds> = testDriver.getKeyValueStore(fundsStoreConfig?.name)
      assertThat(store["1"].balance, `is`(BigDecimal(300)))
    }
  }

  companion object {
    private val OBJECT_MAPPER = ObjectMapper().findAndRegisterModules()
  }
}