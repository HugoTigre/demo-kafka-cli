package com.pakybytes.demo.kafkacli.core.models

import org.apache.kafka.clients.producer.Callback

data class KafkaMsg(val topic: String,
                    val key: String,
                    val msg: String,
                    val callback: Callback? = null)