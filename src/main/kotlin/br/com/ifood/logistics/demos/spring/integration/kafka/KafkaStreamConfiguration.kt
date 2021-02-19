package br.com.ifood.logistics.demos.spring.integration.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import java.util.regex.Pattern

@Configuration
@EnableKafkaStreams
class KafkaStreamConfiguration {

    @Bean
    fun kStream(builder: StreamsBuilder): KStream<String, Long>? {

        val longSerde = Serdes.Long()
        val stringSerde = Serdes.String()

        val stream: KStream<String, String> = builder.stream("GENERIC_TOPIC", Consumed.with(stringSerde, stringSerde))
        val pattern: Pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

        val wordCountStream = stream
                .flatMapValues { _, value -> value.split(pattern) }
                .map { _, word -> KeyValue.pair(word.toUpperCase(), word.toUpperCase()) }
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .count(Materialized.`as`("word-count-state-store"))
                .toStream()

        wordCountStream.to("WORD_COUNT_TOPIC", Produced.with(stringSerde, longSerde))

        return wordCountStream
    }
}
