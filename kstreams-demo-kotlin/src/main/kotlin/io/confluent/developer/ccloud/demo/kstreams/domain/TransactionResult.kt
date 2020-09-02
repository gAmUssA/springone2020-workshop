package io.confluent.developer.ccloud.demo.kstreams.domain

data class TransactionResult(
    var transaction: Transaction,
    var funds: Funds,
    var success: Boolean,
    var errorType: ErrorType?
) {
  enum class ErrorType {
    INSUFFICIENT_FUNDS
  }
}
