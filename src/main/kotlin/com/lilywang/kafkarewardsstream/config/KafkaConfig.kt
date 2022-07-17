package com.lilywang.kafkarewardsstream.config

import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration


@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapAddress: String


    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig():KafkaStreamsConfiguration{
        val props: MutableMap<String, Any> = HashMap()
        props[APPLICATION_ID_CONFIG] = "streams-app"
        props[BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress
//        props[DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
//        props[DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

        return KafkaStreamsConfiguration(props)
    }

}