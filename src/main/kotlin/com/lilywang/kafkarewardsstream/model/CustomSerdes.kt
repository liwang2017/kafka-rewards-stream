package com.lilywang.kafkarewardsstream.model

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.springframework.stereotype.Component
import java.nio.charset.Charset


@Component
class CustomSerdes(
    val transactionSerializer: TransactionSerializer,
    val transactionDeserializer: TransactionDeserializer,
    val totalRewardsSerializer: TotalRewardsSerializer,
    val totalRewardsDeserializer: TotalRewardsDeserializer,
    val dailyRewardsSerializer: DailyRewardsSerializer,
    val dailyRewardsDeserializer: DailyRewardsDeserializer,
    val monthlyRewardsSerializer: MonthlyRewardsSerializer,
    val monthlyRewardsDeserializer: MonthlyRewardsDeserializer,
) {

    fun transactionSerde(): Serde<Transaction> = Serdes.serdeFrom<Transaction>(transactionSerializer, transactionDeserializer)

    fun totalRewardsSerde(): Serde<TotalRewards> = Serdes.serdeFrom<TotalRewards>(totalRewardsSerializer, totalRewardsDeserializer)
    fun dailyRewardsSerde(): Serde<DailyRewards> = Serdes.serdeFrom<DailyRewards>(dailyRewardsSerializer, dailyRewardsDeserializer)

    fun monthlyRewardsSerde(): Serde<MonthlyRewards> = Serdes.serdeFrom<MonthlyRewards>(monthlyRewardsSerializer, monthlyRewardsDeserializer)

}

@Component
class TransactionSerializer(val objectMapper: ObjectMapper): Serializer<Transaction> {

    override fun serialize(topic: String, transaction: Transaction): ByteArray
        = objectMapper.writeValueAsString(transaction).toByteArray(Charset.forName("UTF-8"))

}

@Component
class TransactionDeserializer(val objectMapper: ObjectMapper): Deserializer<Transaction> {

    override fun deserialize(topic: String, data: ByteArray): Transaction
    = objectMapper.readValue(String(data, Charset.forName("UTF-8")), Transaction::class.java)
}

@Component
class TotalRewardsSerializer(val objectMapper: ObjectMapper): Serializer<TotalRewards>{
    override fun serialize(topic: String, totalRewards: TotalRewards): ByteArray
            = objectMapper.writeValueAsString(totalRewards).toByteArray(Charset.forName("UTF-8"))
}

@Component
class TotalRewardsDeserializer(val objectMapper: ObjectMapper): Deserializer<TotalRewards>{
    override fun deserialize(topic: String, data: ByteArray): TotalRewards
            = objectMapper.readValue(String(data, Charset.forName("UTF-8")), TotalRewards::class.java)
}

@Component
class DailyRewardsSerializer(val objectMapper: ObjectMapper): Serializer<DailyRewards>{
    override fun serialize(topic: String, dailyRewards: DailyRewards): ByteArray
            = objectMapper.writeValueAsString(dailyRewards).toByteArray(Charset.forName("UTF-8"))
}

@Component
class DailyRewardsDeserializer(val objectMapper: ObjectMapper): Deserializer<DailyRewards>{
    override fun deserialize(topic: String, data: ByteArray): DailyRewards
            = objectMapper.readValue(String(data, Charset.forName("UTF-8")), DailyRewards::class.java)
}

@Component
class MonthlyRewardsSerializer(val objectMapper: ObjectMapper): Serializer<MonthlyRewards>{
    override fun serialize(topic: String, monthlyRewards: MonthlyRewards): ByteArray
            = objectMapper.writeValueAsString(monthlyRewards).toByteArray(Charset.forName("UTF-8"))
}

@Component
class MonthlyRewardsDeserializer(val objectMapper: ObjectMapper): Deserializer<MonthlyRewards>{
    override fun deserialize(topic: String, data: ByteArray): MonthlyRewards
            = objectMapper.readValue(String(data, Charset.forName("UTF-8")), MonthlyRewards::class.java)
}

