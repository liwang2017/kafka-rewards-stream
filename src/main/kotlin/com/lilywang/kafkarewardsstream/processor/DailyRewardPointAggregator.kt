package com.lilywang.kafkarewardsstream.processor


import com.lilywang.kafkarewardsstream.model.CustomSerdes
import com.lilywang.kafkarewardsstream.model.DailyRewards
import com.lilywang.kafkarewardsstream.model.Transaction
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.time.LocalDate

/**
 * Daily Rewards Stream Processor
 * Key = AccountId#Date
 *
 * Transactions are aggregated by key
 */
@Component
class DailyRewardPointAggregator(
    val customSerdes: CustomSerdes
){
    private val STRING_SERDE = Serdes.String()

    private val log: Logger = LoggerFactory.getLogger(this.javaClass)

    @Autowired
    fun buildPipeline(dailyRewardsKafkaStreamsBuilder: StreamsBuilder) {
        //Consume from topic expense
        val transactionStream: KStream<String, Transaction> = dailyRewardsKafkaStreamsBuilder
            .stream("expenses", Consumed.with(STRING_SERDE, customSerdes.transactionSerde()))

        //Aggregate to DailyRewards by key AccountId#Date
        val dailyRewards: KTable<String, DailyRewards> = transactionStream
            .map { accountId, transaction ->
                KeyValue(
                    "$accountId#${transaction.date.toLocalDate()}",
                    DailyRewards(
                        accountId = transaction.accountId,
                        date = transaction.date.toLocalDate(),
                        rewardsPoint = transaction.amount.toLong()
                    )
                )
            }
            .groupByKey(Grouped.with(Serdes.String(), customSerdes.dailyRewardsSerde()))
            .aggregate(
                this::initialize,
                this::aggregateRewards,
                Materialized.with(STRING_SERDE, customSerdes.dailyRewardsSerde())
            )

        //publish result to topic daily_feed
        dailyRewards.toStream().to("daily_points", Produced.with(Serdes.String(), customSerdes.dailyRewardsSerde()))
    }

    //Setup initial value for aggregator
    private fun initialize(): DailyRewards = DailyRewards("dummyId", LocalDate.now(), 0L)

    private fun aggregateRewards(key: String, newRewards: DailyRewards, aggregatedRewards: DailyRewards): DailyRewards {
        val rewardsPoint = newRewards.rewardsPoint + aggregatedRewards.rewardsPoint
        log.info("AccountId=$key for Date ${newRewards.date}: Calculate ${newRewards.rewardsPoint} + ${aggregatedRewards.rewardsPoint} = $rewardsPoint")
        return DailyRewards(accountId = newRewards.accountId, date = newRewards.date, rewardsPoint = rewardsPoint)
    }
}
