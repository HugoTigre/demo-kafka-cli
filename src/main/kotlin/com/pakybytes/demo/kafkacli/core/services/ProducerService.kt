package com.pakybytes.demo.kafkacli.core.services

import com.pakybytes.demo.kafkacli.core.models.KafkaMsg
import com.pakybytes.demo.kafkacli.data.KafkaProps
import com.pakybytes.demo.kafkacli.di.ServiceLocator
import org.apache.kafka.clients.producer.*
import org.slf4j.LoggerFactory


class ProducerService {

    private val log = LoggerFactory.getLogger(ProducerService::class.java)

    private val repo = ServiceLocator.get().provideKafkaRepo()

    /** Sends a new message every second */
    fun startSending() {

        log.info("Starting sending messages to kafka")

        var key = 1

        while (true) {

            val msg = "Message with key: $key"

            repo.sendAsync(
                    KafkaMsg(
                            KafkaProps.TOPIC_NAME,
                            key.toString(),
                            "Message with key: $key",
                            ProducerCallBack(System.currentTimeMillis(), key, msg)
                    )
            )

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