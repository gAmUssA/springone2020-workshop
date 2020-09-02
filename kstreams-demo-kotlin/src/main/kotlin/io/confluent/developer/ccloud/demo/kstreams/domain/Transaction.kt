package io.confluent.developer.ccloud.demo.kstreams.domain

import java.math.BigDecimal

data class Transaction(
    var guid: String? = null,
    var account: String? = null,
    var amount: BigDecimal? = null,
    var type: Type? = null,
    var currency: String? = null,
    var country: String? = null
) {
  enum class Type {
    DEPOSIT, WITHDRAW
  }
}

