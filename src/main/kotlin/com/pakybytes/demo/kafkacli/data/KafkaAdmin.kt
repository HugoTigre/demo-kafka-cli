package com.pakybytes.demo.kafkacli.data

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import java.util.*

object KafkaProps {

    val LIFECYLCE_TIMEOUT = 10_000L

    val APP_ID = "demo-kafka-cli"
    val SERVER_URL = "127.0.0.1"
    val SERVER_PORT = 9092
    val BOOTSTRAP_SERVER_URL = "$SERVER_URL:$SERVER_PORT"

    // Topics
    val TOPIC_NAME = "demo_topic"
    val TOPIC_PARTITIONS = 1
    val TOPIC_REPLICATION_FACTOR: Short = 1

    // Producer properties
    val LINGER_DATA = "1000" // hold data if less than 1 sec
    val ACK_LEVEL = "1"
    val PRODUCER_RETRIES = "3"

    // Consumer properties
    val CONSUMER_GROUP = "group-1"
    val AUTO_COMMIT = "true" // this should be already by default
    val COMMIT_INTERVAL_MS = "1000" // commits offsets every second
    val SESSION_TIMEOUT_MS_CONFIG = "30000"
    val AUTO_OFFSET_RESET_CONFIG = "earliest" // we want all the messages
    val MAX_POLL_RECORDS = "20"
    val POLL_TIMEOUT = 5000L
}

class KafkaAdmin {

    fun createTopicIfNotExists() {
        try {

            AdminClient.create(buildAdminProperties()).use { admin ->

                val names = admin.listTopics().names().get()

                if (!names.contains(KafkaProps.TOPIC_NAME)) {

                    val newTopic = NewTopic(
                            KafkaProps.TOPIC_NAME,
                            KafkaProps.TOPIC_PARTITIONS,
                            KafkaProps.TOPIC_REPLICATION_FACTOR)

                    admin.createTopics(listOf(newTopic))
                }
            }

        } catch (ex: Exception) {
            throw LMSAPIKafkaAdminException("Error validating/creating kafka topic", ex)
        }
    }


    fun buildAdminProperties(): Properties {

        val props = Properties()

        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVER_URL)

        return props
    }


    private inner class LMSAPIKafkaAdminException constructor(message: String, cause: Throwable) : RuntimeException(message, cause)
}