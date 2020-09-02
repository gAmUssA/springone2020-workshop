package io.confluent.developer.ccloud.demo.kstreams

import io.confluent.developer.ccloud.demo.kstreams.domain.Funds
import io.confluent.developer.ccloud.demo.kstreams.domain.Transaction
import io.confluent.developer.ccloud.demo.kstreams.domain.TransactionResult
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import java.math.BigDecimal

class TransactionTransformer(private val stateStoreName: String)
  : ValueTransformer<Transaction, TransactionResult> {

  private val log = logger<TransactionTransformer>()
  private lateinit var store: KeyValueStore<String, Funds>

  override fun close() {}

  private fun createEmptyFunds(account: String): Funds {
    val funds = Funds(account, BigDecimal.ZERO)
    store.put(account, funds)
    return funds
  }

  private fun depositFunds(transaction: Transaction): Funds {
    return updateFunds(transaction.account, transaction.amount)
  }

  private fun getFunds(account: String?): Funds {
    return store.get(account) ?: return createEmptyFunds(account!!)
  }

  private fun hasEnoughFunds(transaction: Transaction): Boolean {
    return getFunds(transaction.account!!).balance.compareTo(transaction.amount) !== -1
  }

  override fun init(context: ProcessorContext) {
    store = context.getStateStore(stateStoreName) as KeyValueStore<String, Funds>
  }

  override fun transform(transaction: Transaction): TransactionResult {
    if (transaction.type?.equals(Transaction.Type.DEPOSIT)!!) {
      return TransactionResult(transaction,
          depositFunds(transaction),
          true,
          null)
    }
    if (hasEnoughFunds(transaction)) {
      return TransactionResult(transaction, withdrawFunds(transaction), true, null)
    }
    log.info("Not enough funds for account {}.", transaction.account)
    return TransactionResult(transaction,
        getFunds(transaction.account),
        false,
        TransactionResult.ErrorType.INSUFFICIENT_FUNDS)
  }

  private fun updateFunds(account: String?, amount: BigDecimal?): Funds {
    val funds = Funds(account, getFunds(account).balance.add(amount))

    log.info("Updating funds for account {} with {}. Current balance is {}.", account, amount, funds.balance)
    store.put(account, funds)
    return funds
  }

  private fun withdrawFunds(transaction: Transaction): Funds {
    return updateFunds(transaction.account, transaction.amount?.negate())
  }
}