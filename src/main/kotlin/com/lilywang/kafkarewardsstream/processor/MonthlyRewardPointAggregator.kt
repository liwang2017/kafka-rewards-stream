package com.lilywang.kafkarewardsstream.processor


import com.lilywang.kafkarewardsstream.model.CustomSerdes
import com.lilywang.kafkarewardsstream.model.MonthlyRewards
import com.lilywang.kafkarewardsstream.model.Transaction
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.time.YearMonth

/**
 * Monthly Rewards Stream Processor
 * Key = AccountId#YearMonth
 *
 * Transactions are aggregated by key
 */
@Component
class MonthlyRewardPointAggregator(
    val customSerdes: CustomSerdes
){
    private val STRING_SERDE = Serdes.String()

    private val log: Logger = LoggerFactory.getLogger(this.javaClass)

    @Autowired
    fun buildPipeline(monthlyRewardsKafkaStreamsBuilder: StreamsBuilder) {
        //Consume from topic expense
        val transactionStream: KStream<String, Transaction> = monthlyRewardsKafkaStreamsBuilder
            .stream("expenses", Consumed.with(STRING_SERDE, customSerdes.transactionSerde()))

        //Aggregate to MonthlyRewards by key AccountId#YearMonth
        val monthlyRewards: KTable<String, MonthlyRewards> = transactionStream
            .map { accountId, transaction ->
                KeyValue(
                    "$accountId#${transaction.date.let{YearMonth.of(it.year, it.monthValue).toString()}}",
                    MonthlyRewards(
                        accountId = transaction.accountId,
                        month = YearMonth.of(transaction.date.year, transaction.date.monthValue),
                        rewardsPoint = transaction.amount.toLong()
                    )
                )
            }
            .groupByKey(Grouped.with(Serdes.String(), customSerdes.monthlyRewardsSerde()))
            .aggregate(
                this::initialize,
                this::aggregateRewards,
                Materialized.with(STRING_SERDE, customSerdes.monthlyRewardsSerde())
            )

        //publish result to topic monthly_feed
        monthlyRewards.toStream().to("monthly_feed", Produced.with(Serdes.String(), customSerdes.monthlyRewardsSerde()))
    }

    //Setup initial value for aggregator
    private fun initialize(): MonthlyRewards = MonthlyRewards("dummyId", YearMonth.now(), 0L)

    private fun aggregateRewards(key: String, newRewards: MonthlyRewards, aggregatedRewards: MonthlyRewards): MonthlyRewards {
        val rewardsPoint = newRewards.rewardsPoint + aggregatedRewards.rewardsPoint
        log.info("AccountId=$key for Month ${newRewards.month}: Calculate ${newRewards.rewardsPoint} + ${aggregatedRewards.rewardsPoint} = $rewardsPoint")
        return MonthlyRewards(accountId = newRewards.accountId, month = newRewards.month, rewardsPoint = rewardsPoint)
    }
}