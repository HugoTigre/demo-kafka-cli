package com.pakybytes.demo.kafkacli.core.services

import com.pakybytes.demo.kafkacli.core.models.KafkaMsg
import net.jodah.failsafe.RetryPolicy
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*
import java.util.concurrent.Future


interface KafkaRepoGateway {

    fun sendAsync(msg: KafkaMsg): Future<RecordMetadata>

    fun read(topic: String): Optional<ConsumerRecords<String, String>>

    fun commitAsync(offsets: Map<TopicPartition, OffsetAndMetadata>,
                    retryPolicy: RetryPolicy<KafkaConsumer<String, String>>? = null)
}