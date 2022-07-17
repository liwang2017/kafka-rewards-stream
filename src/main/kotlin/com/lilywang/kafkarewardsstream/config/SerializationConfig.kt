package com.lilywang.kafkarewardsstream.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


@Configuration
class SerializationConfig {

    @Bean
    fun objectMapper(): ObjectMapper {
        val module = JavaTimeModule()
        val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        module.addDeserializer(LocalDateTime::class.java, LocalDateTimeDeserializer(dateFormat))
        module.addSerializer(LocalDateTime::class.java, LocalDateTimeSerializer(dateFormat))
        return Jackson2ObjectMapperBuilder.json()
            .modules(
                module,
                Jdk8Module(),
                ParameterNamesModule()
            )
            .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build()
    }
}