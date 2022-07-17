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
    val totalRewardsDeserializer: TotalRewardsDeserializer
) {

    fun transactionSerde(): Serde<Transaction> = Serdes.serdeFrom<Transaction>(transactionSerializer, transactionDeserializer)

    fun totalRewardsSerde(): Serde<TotalRewards> = Serdes.serdeFrom<TotalRewards>(totalRewardsSerializer, totalRewardsDeserializer)
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

