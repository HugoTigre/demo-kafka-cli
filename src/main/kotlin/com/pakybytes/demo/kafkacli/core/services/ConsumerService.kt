package com.pakybytes.demo.kafkacli.core.services

import com.pakybytes.demo.kafkacli.core.models.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ConsumerService {

    private val log = LoggerFactory.getLogger(ConsumerService::class.java)

    fun buildConsumer() = KafkaConsumer<Int, String>(buildProperties())


    fun buildProperties(): Properties {

        val props = Properties()

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "${KafkaProperties.SERVER_URL}:${KafkaProperties.SERVER_PORT}")

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer::class.qualifiedName)
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaProperties.CONSUMER_GROUP)

        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaProperties.AUTO_COMMIT)

        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KafkaProperties.COMMIT_INTERVAL_MS)

        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafkaProperties.SESSION_TIMEOUT_MS_CONFIG)

        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaProperties.AUTO_OFFSET_RESET_CONFIG)

        return props
    }


    /** Reads the messages from kafka each second, starting at previous offset */
    fun startReading(consumer: KafkaConsumer<Int, String>, topic: String) {

        log.info("--- Reading data from kafka ---")

        consumer.subscribe(Collections.singletonList(topic))

        while (true) {

            val consumerRecord = consumer.poll(Duration.ofMillis(1000))

            consumerRecord.forEach {
                log.info("CONSUMER <- key: ${it.key()}, value: ${it.value()}, partition ${it.partition()}, offset: ${it.offset()}, timestamp: ${it.timestamp()}")
            }
        }
    }

}