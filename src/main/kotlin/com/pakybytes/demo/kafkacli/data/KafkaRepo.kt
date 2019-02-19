package com.pakybytes.demo.kafkacli.data

import com.pakybytes.demo.kafkacli.core.models.KafkaMsg
import com.pakybytes.demo.kafkacli.core.services.KafkaRepoGateway
import net.jodah.failsafe.Failsafe
import net.jodah.failsafe.RetryPolicy
import net.jodah.failsafe.function.CheckedRunnable
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Future


class KafkaRepo : KafkaRepoGateway {

    private val log = LoggerFactory.getLogger(KafkaRepo::class.java)

    private val producer: KafkaProducer<String, String>
    private val consumer: KafkaConsumer<String, String>

    private val retryPolicy: RetryPolicy<KafkaConsumer<String, String>>

    init {
        this.producer = KafkaProducer(buildProducerProperties())
        this.consumer = KafkaConsumer(buildConsumerProperties())

        this.retryPolicy = RetryPolicy<KafkaConsumer<String, String>>()
                .withBackoff(2000, 6000, ChronoUnit.MILLIS)
                .withMaxRetries(3)
                .onFailedAttempt { ctx -> log.error("Connection attempt failed", ctx.lastFailure) }
                .onRetry { ctx -> log.warn("Failure #{}. Retrying.", ctx.attemptCount) }
    }


    /**
     *  Asynchronously send a record to a topic, if the [KafkaMsg] contains
     *  a callback, it used that callback to process the response, if not, just
     *  sends the msg.
     */
    override
    fun sendAsync(msg: KafkaMsg): Future<RecordMetadata> {

        val record = ProducerRecord<String, String>(msg.topic, msg.key, msg.msg)

        return producer.send(record, msg.callback)
    }


    /**
     * @return max of [KafkaProps.MAX_POLL_RECORDS] results or
     * [Optional.empty] if no value exists after committed offset.
     */
    override
    fun read(topic: String): Optional<ConsumerRecords<String, String>> {

        consumer.subscribe(listOf(topic))

        val consumerRecords = consumer.poll(Duration.ofMillis(KafkaProps.POLL_TIMEOUT))

        return if (consumerRecords.isEmpty) Optional.empty() else Optional.of(consumerRecords)
    }


    /**
     * Commits the offset asynchronously. Uses a retry function in case anything goes wrong
     * at first try. If a RetryPolicy is not passed as a parameter it uses a default one, which
     * tries 3 times in one minute.
     */
    override
    fun commitAsync(offsets: Map<TopicPartition, OffsetAndMetadata>,
                    retryPolicy: RetryPolicy<KafkaConsumer<String, String>>?) {

        val rp = if (retryPolicy == null) this.retryPolicy else retryPolicy

        Failsafe.with<KafkaConsumer<String, String>, RetryPolicy<KafkaConsumer<String, String>>>(rp)
                .run(CheckedRunnable {
                    consumer.commitAsync(offsets) { offsets1, e -> if (e != null) log.error("Commit failed for offsets {}", offsets1, e) }
                })
    }


    fun buildProducerProperties(): Properties {

        val props = Properties()

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVER_URL)

        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, KafkaProps.APP_ID)

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        props.setProperty(ProducerConfig.ACKS_CONFIG, KafkaProps.ACK_LEVEL)

        props.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaProps.PRODUCER_RETRIES)

        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaProps.LINGER_DATA)

        return props
    }


    fun buildConsumerProperties(): Properties {

        val props = Properties()

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVER_URL)

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaProps.CONSUMER_GROUP)

        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaProps.AUTO_COMMIT)

        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KafkaProps.COMMIT_INTERVAL_MS)

        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, KafkaProps.SESSION_TIMEOUT_MS_CONFIG)

        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaProps.AUTO_OFFSET_RESET_CONFIG)

        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaProps.MAX_POLL_RECORDS)

        return props
    }

}