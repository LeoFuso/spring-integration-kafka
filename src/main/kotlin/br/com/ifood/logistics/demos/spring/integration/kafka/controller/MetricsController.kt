package br.com.ifood.logistics.demos.spring.integration.kafka.controller


import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("metrics")
class MetricsController(private val factory: StreamsBuilderFactoryBean) {


    @GetMapping(path = ["/{key}"], produces = [MediaType.APPLICATION_JSON_VALUE])
    suspend fun inspect(@PathVariable key: String): ResponseEntity<Long> {

        val type = QueryableStoreTypes.keyValueStore<String, Long>()
        val queryStore = StoreQueryParameters.fromNameAndType("word-count-state-store", type)
        val valueStore = factory.kafkaStreams.store(queryStore)

        val wordCount = valueStore[key]

        return wordCount.let {
            ResponseEntity.ok(it)
        }

    }

}