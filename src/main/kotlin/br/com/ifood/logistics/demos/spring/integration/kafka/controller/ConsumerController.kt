package br.com.ifood.logistics.demos.spring.integration.kafka.controller

import org.springframework.http.ResponseEntity
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController


@RestController
@RequestMapping("consumer")
@Suppress("SpringJavaInjectionPointsAutowiringInspection")
class ConsumerController(private val listener: KafkaListenerEndpointRegistry) {

    @PutMapping(path = ["/{topic}/stop/"])
    suspend fun stop(@PathVariable topic: String): ResponseEntity<String> {

        val container = listener.allListenerContainers
                .find {
                    val properties = it.containerProperties
                    properties.topics?.contains(topic) ?: false
                }

        if (!isContainerPaused(container)) {
            container?.pause()
            return ResponseEntity
                    .ok()
                    .build()
        }

        return ResponseEntity
                .badRequest()
                .build()

    }

    @PutMapping(path = ["/{topic}/stop"])
    suspend fun start(@PathVariable topic: String): ResponseEntity<String> {

        val container = listener.allListenerContainers
                .find {
                    val properties = it.containerProperties
                    properties.topics?.contains(topic) ?: false
                }

        if (isContainerPaused(container)) {
            container?.resume()
            return ResponseEntity
                    .ok()
                    .build()
        }

        return ResponseEntity
                .badRequest()
                .build()
    }

    private fun isContainerPaused(container: MessageListenerContainer?): Boolean {
        if (container is ConcurrentMessageListenerContainer<*, *>) {
            return container.isPauseRequested && container.isContainerPaused
        }
        return false
    }
}