package com.lilywang.kafkarewardsstream.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer
import com.fasterxml.jackson.datatype.jsr310.deser.YearMonthDeserializer
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer
import com.fasterxml.jackson.datatype.jsr310.ser.YearMonthSerializer
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.YearMonth
import java.time.format.DateTimeFormatter


@Configuration
class SerializationConfig {

    /**
     * Setup ObjectMapper bean for Serialization/Deserialization
     */
    @Bean
    fun objectMapper(): ObjectMapper {
        val module = JavaTimeModule()
        val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val yearMonthFormat = DateTimeFormatter.ofPattern("uuuu-MM")

        module.addDeserializer(LocalDateTime::class.java, LocalDateTimeDeserializer(dateTimeFormat))
        module.addSerializer(LocalDateTime::class.java, LocalDateTimeSerializer(dateTimeFormat))
        module.addDeserializer(LocalDate::class.java, LocalDateDeserializer(dateFormat))
        module.addSerializer(LocalDate::class.java, LocalDateSerializer(dateFormat))
        module.addDeserializer(YearMonth::class.java, YearMonthDeserializer(yearMonthFormat))
        module.addSerializer(YearMonth::class.java, YearMonthSerializer(yearMonthFormat))
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