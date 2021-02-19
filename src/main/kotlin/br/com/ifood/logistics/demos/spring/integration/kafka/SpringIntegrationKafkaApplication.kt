package br.com.ifood.logistics.demos.spring.integration.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.support.beans
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.TopicBuilder
import org.springframework.web.reactive.config.EnableWebFlux


@EnableKafka
@EnableWebFlux
@SpringBootApplication
class SpringIntegrationKafkaApplication

fun main(args: Array<String>) {
    runApplication<SpringIntegrationKafkaApplication>(*args) {
        addInitializers(
                beans {
                    bean("genericTopic") {
                        TopicBuilder.name("GENERIC_TOPIC")
                                .partitions(3)
                                .build()
                    }
                    bean("genericTopicDLQ") {
                        TopicBuilder.name("GENERIC_TOPIC.DLT")
                                .partitions(3)
                                .build()
                    }
                    bean("wordCountTopic") {
                        TopicBuilder.name("WORD_COUNT_TOPIC")
                                .partitions(3)
                                .build()
                    }
                }
        )
    }
}