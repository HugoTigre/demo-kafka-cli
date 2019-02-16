package com.pakybytes.demo.kafkacli.core.services

import com.pakybytes.demo.kafkacli.core.models.KafkaMsg
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*


interface KafkaRepoGateway {

    fun sendAsync(msg: KafkaMsg)

    fun read(topic: String): Optional<ConsumerRecords<String, String>>

    fun commitAsync(offsets: Map<TopicPartition, OffsetAndMetadata>)
}