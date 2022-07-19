package com.lilywang.kafkarewardsstream.model

import com.lilywang.kafkarewardsstream.annotation.NoArg
import java.time.LocalDate
import java.time.YearMonth

@NoArg
data class TotalRewards (
    val accountId: String,
    val rewardsPoint: Long
)

data class MonthlyRewards(
    val accountId: String,
    val month: YearMonth?,
    val rewardsPoint: Long
)

data class DailyRewards(
    val accountId: String,
    val date: LocalDate,
    val rewardsPoint: Long
)
