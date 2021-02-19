package br.com.ifood.logistics.demos.spring.integration.kafka.listener.configuration

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import org.springframework.util.backoff.FixedBackOff
import java.time.Duration
import javax.annotation.PostConstruct

@Configuration
class KafkaListenerContainerConfiguration(
        private val factory: KafkaListenerContainerFactory<*>,
        private val template: KafkaOperations<*, *>,
) {

    @PostConstruct
    fun additionalKafkaListenerContainerFactoryConfiguration() {

        val containerFactory = factory as ConcurrentKafkaListenerContainerFactory<*, *>

        val recoverPolicy =
            DeadLetterPublishingRecoverer(template) {
                r: ConsumerRecord<*, *>, _: Exception -> TopicPartition("${r.topic()}.DLT", r.partition())
            }

        val backOffInterval = Duration.ofSeconds(3).toMillis()
        val backOff = FixedBackOff(backOffInterval, 2)
        val errorHandler = SeekToCurrentErrorHandler(recoverPolicy, backOff)


        containerFactory.setErrorHandler(errorHandler)
        containerFactory.containerProperties.isSyncCommits = true
    }
}