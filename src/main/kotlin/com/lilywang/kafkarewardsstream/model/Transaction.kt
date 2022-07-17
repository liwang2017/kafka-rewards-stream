package com.lilywang.kafkarewardsstream.model

import com.lilywang.kafkarewardsstream.annotation.NoArg
import java.math.BigDecimal
import java.time.LocalDateTime


@NoArg
data class Transaction (
    val transactionId: String,
    val date: LocalDateTime,
    val accountId: String,
    val amount: BigDecimal,
)

