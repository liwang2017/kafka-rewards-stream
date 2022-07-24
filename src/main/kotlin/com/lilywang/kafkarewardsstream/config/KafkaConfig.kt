package com.lilywang.kafkarewardsstream.config

import org.apache.kafka.streams.StreamsConfig.*
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer
import java.util.*
import kotlin.collections.HashMap
import kotlin.collections.HashSet


@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaConfig {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(kafkaConfigProperties: Properties):KafkaStreamsConfiguration
    {
        val props: MutableMap<String, Any> = HashMap()
        props[APPLICATION_ID_CONFIG] = "total-rewards-stream"
        props.putAll(kafkaConfigProperties.toMap() as Map<String, String>)
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun dailyRewardsStreamConfig(kafkaConfigProperties: Properties): KafkaStreamsConfiguration
    {
        val props: MutableMap<String, Any> = HashMap()
        props[APPLICATION_ID_CONFIG] = "daily-rewards-stream"
        props.putAll(kafkaConfigProperties.toMap() as Map<String, String>)
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun monthlyRewardsStreamConfig(kafkaConfigProperties: Properties): KafkaStreamsConfiguration
    {
        val props: MutableMap<String, Any> = HashMap()
        props[APPLICATION_ID_CONFIG] = "monthly-rewards-stream"
        props.putAll(kafkaConfigProperties.toMap() as Map<String, String>)
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun dailyRewardsKafkaStreamsBuilder(
        dailyRewardsStreamConfig: KafkaStreamsConfiguration,
        customizerProvider: ObjectProvider<StreamsBuilderFactoryBeanCustomizer>,
        configurerProvider: ObjectProvider<StreamsBuilderFactoryBeanConfigurer>
    ): StreamsBuilderFactoryBean {

        val fb = StreamsBuilderFactoryBean(dailyRewardsStreamConfig)
        val configuredBy: MutableSet<StreamsBuilderFactoryBeanConfigurer> = HashSet<StreamsBuilderFactoryBeanConfigurer>()
        configurerProvider.orderedStream().forEach { configurer: StreamsBuilderFactoryBeanConfigurer ->
            configurer.configure(fb)
            configuredBy.add(configurer)
        }
        val customizer = customizerProvider.ifUnique
        if (customizer != null && !configuredBy.contains(customizer)) {
            customizer.configure(fb)
        }
        return fb
    }

    @Bean
    fun monthlyRewardsKafkaStreamsBuilder(
        monthlyRewardsStreamConfig: KafkaStreamsConfiguration,
        customizerProvider: ObjectProvider<StreamsBuilderFactoryBeanCustomizer>,
        configurerProvider: ObjectProvider<StreamsBuilderFactoryBeanConfigurer>
    ): StreamsBuilderFactoryBean {
        val fb = StreamsBuilderFactoryBean(monthlyRewardsStreamConfig)
        val configuredBy: MutableSet<StreamsBuilderFactoryBeanConfigurer> = HashSet<StreamsBuilderFactoryBeanConfigurer>()
        configurerProvider.orderedStream().forEach { configurer: StreamsBuilderFactoryBeanConfigurer ->
            configurer.configure(fb)
            configuredBy.add(configurer)
        }
        val customizer = customizerProvider.ifUnique
        if (customizer != null && !configuredBy.contains(customizer)) {
            customizer.configure(fb)
        }
        return fb
    }

    @Bean
    @ConfigurationProperties(prefix = "kafka")
    fun kafkaConfigProperties() = Properties()
}

