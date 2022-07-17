package com.lilywang.kafkarewardsstream.controller

import com.lilywang.kafkarewardsstream.model.TotalRewards
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class QueryController(val factoryBean: StreamsBuilderFactoryBean) {

    @GetMapping("/rewards/{id}")
    fun getTotalRewards(@PathVariable id: String): TotalRewards? {
        val kafkaStreams: KafkaStreams = factoryBean.getKafkaStreams()
        val rewards = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType("rewards-total", QueryableStoreTypes.keyValueStore<String, TotalRewards>())
        )
        return rewards[id]
    }
}

