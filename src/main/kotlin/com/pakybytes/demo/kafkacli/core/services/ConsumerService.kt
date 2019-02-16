package com.pakybytes.demo.kafkacli.core.services

import com.pakybytes.demo.kafkacli.data.KafkaProps
import com.pakybytes.demo.kafkacli.di.ServiceLocator
import org.slf4j.LoggerFactory

class ConsumerService {

    private val log = LoggerFactory.getLogger(ConsumerService::class.java)

    private val repo = ServiceLocator.get().kafkaRepo

    /** Reads the messages from kafka each second, starting at previous offset */
    fun startReading() {

        log.info("--- Reading data from kafka ---")

        while (true) {

            val consumerRecords = repo.read(KafkaProps.TOPIC_NAME)

            consumerRecords.map {
                it.forEach {
                    log.info("CONSUMER <- key: ${it.key()}, value: ${it.value()}, partition ${it.partition()}, offset: ${it.offset()}, timestamp: ${it.timestamp()}")
                }
            }
        }
    }

}