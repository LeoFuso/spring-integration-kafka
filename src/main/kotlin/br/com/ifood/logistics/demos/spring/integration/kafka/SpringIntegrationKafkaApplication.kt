package br.com.ifood.logistics.demos.spring.integration.kafka

import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.beans
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Headers
import org.springframework.stereotype.Component
import org.springframework.util.backoff.FixedBackOff
import org.springframework.web.bind.annotation.*
import org.springframework.web.reactive.config.EnableWebFlux
import java.time.Duration
import javax.annotation.PostConstruct


@EnableWebFlux
@SpringBootApplication
class SpringIntegrationKafkaApplication

fun main(args: Array<String>) {
    runApplication<SpringIntegrationKafkaApplication>(*args) {
        addInitializers(
            beans {
                bean("genericTopic") {
                    TopicBuilder.name("GENERIC_TOPIC")
                        .build()
                }
                bean("genericTopicDLQ") {
                    TopicBuilder.name("GENERIC_TOPIC.DLT")
                        .build()
                }
            }
        )
    }
}


@EnableKafka
@Configuration
class KafkaListenerContainerConfiguration(
    val factory: KafkaListenerContainerFactory<*>,
    val template: KafkaOperations<*, *>
) {

    @PostConstruct
    fun additionalKafkaListenerContainerFactoryConfiguration() {
        val containerFactory = factory as ConcurrentKafkaListenerContainerFactory<*, *>

        val recoverPolicy = DeadLetterPublishingRecoverer(template)
        { r: ConsumerRecord<*, *>, _: Exception ->
            TopicPartition("${r.topic()}.DLT", r.partition())
        }

        val backOffInterval = Duration.ofSeconds(3).toMillis()
        val backOff = FixedBackOff(backOffInterval, 2)
        val errorHandler = SeekToCurrentErrorHandler(recoverPolicy, backOff)


        containerFactory.setErrorHandler(errorHandler)
        containerFactory.containerProperties.isSyncCommits = true
    }
}


@RestController
@RequestMapping("kafka")
class KafkaController(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    @Suppress("SpringJavaInjectionPointsAutowiringInspection") private val listener: KafkaListenerEndpointRegistry
) {

    @PostMapping(path = ["/v1/{topic}"], produces = [MediaType.APPLICATION_JSON_VALUE])
    suspend fun postMessage(@PathVariable topic: String, @RequestBody message: String): ResponseEntity<String> {
        return kafkaTemplate.send(topic, message)
            .completable()
            .await()
            .let {
                ResponseEntity.ok()
                    .body(it.toString())
            }
    }

    @PutMapping(path = ["/v1/{topic}"], produces = [MediaType.APPLICATION_JSON_VALUE])
    suspend fun stopResumeTopic(@PathVariable topic: String): ResponseEntity<*> {
        return listener.allListenerContainers.stream()
            .filter {
                val containerProperties = it.containerProperties
                val topics = containerProperties.topics ?: emptyArray()
                topics.contains(topic)
            }.map {
                it as ConcurrentMessageListenerContainer<*, *>
            }.map {
                if (it.isPauseRequested) it.resume() else it.pause()
                it.isPauseRequested
            }.reduce { acc: Boolean, nothing: Boolean ->
                acc && nothing
            }.map {
                ResponseEntity.ok(it)
            }.orElseThrow()
    }
}

@Component
class KafkaListener {


    @KafkaListener(topics = ["GENERIC_TOPIC"])
    fun consume(message: String, ack: Acknowledgment) {

        fun printlnLog(message: String) {
            println("${Thread.currentThread().name} -> $message")
        }

        printlnLog(message)
        runBlocking {
            printlnLog("Delaying the commit. You can shutdown the Application now!")
            delay(10_000)
        }

        if (message.contains("ERROR")) {
            throw RuntimeException()
        }


        ack.acknowledge()
        printlnLog("Ok, message committed.")
    }

    @KafkaListener(topics = ["GENERIC_TOPIC.DLT"])
    fun consumeDLQ(
        message: ConsumerRecord<String, String>,
        @Headers headers: Map<KafkaHeaders, *>,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        println("${Thread.currentThread().name} [ $offset ] -> $message")
        headers.forEach { println(it.toString()) }
    }

}