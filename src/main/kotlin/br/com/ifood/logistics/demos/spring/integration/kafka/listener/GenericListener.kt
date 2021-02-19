package br.com.ifood.logistics.demos.spring.integration.kafka.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Headers
import org.springframework.stereotype.Component

@Component
class GenericListener {

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(GenericListener::class.java)
    }

    @KafkaListener(topics = ["GENERIC_TOPIC"])
    fun consume(message: String, ack: Acknowledgment, @Headers headers: Map<String, *>) {
        LOGGER.info(
                "${headers[KafkaHeaders.TOPIC]}:[${headers[KafkaHeaders.PARTITION_ID]}][${headers[KafkaHeaders.OFFSET]}] -> $message"
        )
        ack.acknowledge()
    }

    @KafkaListener(topics = ["GENERIC_TOPIC.DLT"])
    fun consumeDLQ(message: ConsumerRecord<String, String>, @Headers headers: Map<String, *>, ack: Acknowledgment) {
        println("${Thread.currentThread().name} [ ${headers[KafkaHeaders.OFFSET]} ] -> $message")
        ack.acknowledge()
    }

    @KafkaListener(topics = ["WORD_COUNT_TOPIC"])
    fun consumeWordCount(message: String, ack: Acknowledgment, @Headers headers: Map<String, *>) {
        LOGGER.info(
                "${headers[KafkaHeaders.TOPIC]}:[${headers[KafkaHeaders.PARTITION_ID]}][${headers[KafkaHeaders.OFFSET]}] -> $message"
        )
        ack.acknowledge()
    }

}