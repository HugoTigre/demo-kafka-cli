package com.pakybytes.demo.kafkacli.core.models


object KafkaProperties {

    val APP_ID = "demo-kafka-cli"

    // Producer properties
    val SERVER_URL = "127.0.0.1"
    val SERVER_PORT = 9092
    val LINGER_DATA = "1000" // hold data if less than 1 sec
    val ACK_LEVEL = "1"
    val PRODUCER_RETRIES = "3"

    // Consumer properties
    val CONSUMER_GROUP = "group-1"
    val AUTO_COMMIT = "true" // this should be already by default
    val COMMIT_INTERVAL_MS = "1000" // commits offsets every second
    val SESSION_TIMEOUT_MS_CONFIG = "30000"
    val AUTO_OFFSET_RESET_CONFIG = "earliest" // we want all the messages
}