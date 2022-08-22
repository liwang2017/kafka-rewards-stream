package com.lilywang.kafkarewardsstream.processor


import com.lilywang.kafkarewardsstream.model.CustomSerdes
import com.lilywang.kafkarewardsstream.model.TotalRewards
import com.lilywang.kafkarewardsstream.model.Transaction
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

/**
 * Total Rewards Stream Processor
 * Key = AccountId
 *
 * Transactions are aggregated by key
 */
@Component
class TotalRewardPointAggregator(
    val customSerdes: CustomSerdes
){
    private val STRING_SERDE = Serdes.String()

    private val log: Logger = LoggerFactory.getLogger(this.javaClass)

    //Use different stream builder
    @Autowired
    fun buildPipeline(defaultKafkaStreamsBuilder: StreamsBuilder) {
        val transactionStream: KStream<String, Transaction> = defaultKafkaStreamsBuilder
            .stream("expenses", Consumed.with(STRING_SERDE, customSerdes.transactionSerde()))

        val totalRewards: KTable<String, TotalRewards> = transactionStream
            .mapValues { transaction ->
                TotalRewards(
                    accountId = transaction.accountId,
                    rewardsPoint = transaction.amount.toLong()
                )
            }
            .groupByKey(Grouped.with(Serdes.String(), customSerdes.totalRewardsSerde()))
            .aggregate(
                this::initialize,
                this::aggregateRewards,
                Materialized.with(STRING_SERDE, customSerdes.totalRewardsSerde())
            )

        totalRewards.toStream().to("total_points", Produced.with(Serdes.String(), customSerdes.totalRewardsSerde()))
    }

    private fun initialize(): TotalRewards = TotalRewards("dummyId", 0L)

    private fun aggregateRewards(key: String, newRewards: TotalRewards, aggregatedRewards: TotalRewards): TotalRewards {
        val rewardsPoint = newRewards.rewardsPoint + aggregatedRewards.rewardsPoint
        log.info("AccountId=$key: Calculate ${newRewards.rewardsPoint} + ${aggregatedRewards.rewardsPoint} = $rewardsPoint")
        return TotalRewards(accountId = key, rewardsPoint = rewardsPoint)
    }
}
