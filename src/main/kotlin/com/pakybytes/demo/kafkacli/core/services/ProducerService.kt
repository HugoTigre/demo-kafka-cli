package com.pakybytes.demo.kafkacli.core.services

import com.pakybytes.demo.kafkacli.core.models.KafkaProperties
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*


class ProducerService {

    private val log = LoggerFactory.getLogger(ProducerService::class.java)

    fun buildProducer() = KafkaProducer<Int, String>(buildProperties())

    private
    fun buildProperties(): Properties {

        val props = Properties()

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${KafkaProperties.SERVER_URL}:${KafkaProperties.SERVER_PORT}")

        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, KafkaProperties.APP_ID)

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer::class.qualifiedName)
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)

        props.setProperty(ProducerConfig.ACKS_CONFIG, KafkaProperties.ACK_LEVEL)

        props.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaProperties.PRODUCER_RETRIES)

        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaProperties.LINGER_DATA)

        return props
    }


    /** Sends a new message every second */
    fun startSending(producer: KafkaProducer<Int, String>, topic: String) {

        log.info("Starting sending messages to kafka")

        var key = 1

        while (true) {

            val msg = "Message with key: $key"

            producer.send(
                    ProducerRecord<Int, String>(topic, key, msg),
                    ProducerCallBack(System.currentTimeMillis(), key, msg))

            ++key

            Thread.sleep(1000)
        }
    }
}

internal class ProducerCallBack(private val startTime: Long, private val key: Int, private val msg: String) : Callback {

    private val log = LoggerFactory.getLogger(ProducerCallBack::class.java)

    override
    fun onCompletion(metadata: RecordMetadata?, ex: Exception?) {
        val elapsedTime = System.currentTimeMillis() - startTime
        if (metadata != null) {
            log.info("PRODUCER -> key: $key, msg: $msg, partition: ${metadata.partition()}, offset: ${metadata.offset()}, elapsedTime: $elapsedTime ms")
        } else {
            log.error("Error retrieving producer response", ex)
        }
    }
}