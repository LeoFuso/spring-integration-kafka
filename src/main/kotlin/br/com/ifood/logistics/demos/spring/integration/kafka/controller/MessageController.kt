package br.com.ifood.logistics.demos.spring.integration.kafka.controller

import kotlinx.coroutines.future.await
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.GenericMessage
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@RestController
@RequestMapping("message")
class MessageController(private val kafkaTemplate: KafkaTemplate<String, String>) {

    companion object {
        private const val DEFAULT_TOPIC = "GENERIC_TOPIC"
    }

    @PostMapping(
            path = ["enqueue"],
            consumes = [MediaType.TEXT_PLAIN_VALUE],
            produces = [MediaType.TEXT_PLAIN_VALUE]
    )
    suspend fun enqueue(
            @RequestBody message: String,
            @RequestParam(name = "topic", defaultValue = DEFAULT_TOPIC) topic: String,
            @RequestParam(name = "partition", required = false) partition: Int?,
    ): ResponseEntity<String> {

        val headers = mapOf<String, Any?>(
                KafkaHeaders.TOPIC to topic,
                KafkaHeaders.PARTITION_ID to partition,
                KafkaHeaders.MESSAGE_KEY to UUID.randomUUID().toString()
        )
        val record = GenericMessage(message, headers)

        try {

            val sendResult = kafkaTemplate.send(record)
                    .completable()
                    .orTimeout(5, TimeUnit.SECONDS)
                    .await()

            val metadata = sendResult.recordMetadata

            return ResponseEntity.ok("[${metadata.partition()}][${metadata.offset()}] -> $message")

        } catch (ex: TimeoutException) {

            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("${ex::class.simpleName} -> ${ex.message}")

        } catch (ex: Exception) {

            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("${ex::class.simpleName} -> ${ex.message}")

        }
    }
}