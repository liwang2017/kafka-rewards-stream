package com.lilywang.kafkarewardsstream

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaStreamApplication

fun main(args: Array<String>) {
	runApplication<KafkaStreamApplication>(*args)
}
