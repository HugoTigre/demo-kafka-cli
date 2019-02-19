package com.pakybytes.demo.kafkacli.data

import com.pakybytes.demo.kafkacli.core.models.KafkaMsg
import common.IntegrationTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.*



@IntegrationTest
@DisplayName("KafkaRepo integration tests")
internal class KafkaRepoTest {


    @Test
    fun `sendAsync should send data to kafka asynchronously`() {
        isKafkaOnline()
        assertTrue(!repo.sendAsync(kafkaMsg).isDone)
    }

    @Test
    fun `sendAsync should send data to kafka and return metadata`() {
        isKafkaOnline()
        val metadata = repo.sendAsync(kafkaMsg).get()
        assertTrue(metadata.topic().equals(topic))
    }

    @Test
    fun `commitAsync should commit previously read offsets`() {
        isKafkaOnline()

        var ok = false

        val consumerRecords = repo.read(topic)

        consumerRecords.ifPresent { records ->

            val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()

            records.forEach { record ->

                currentOffsets.put(
                        TopicPartition(record.topic(), record.partition()),
                        OffsetAndMetadata(record.offset() + 1, "no metadata"))
            }

            repo.commitAsync(currentOffsets)

            Thread.sleep(2000) // gives 2 seconds for the commit, workaround for a asynchronous commit

            val newConsumerRecords = repo.read(topic)

            ok = !newConsumerRecords.isPresent // should be empty
        }

        assertTrue(ok)
    }


    @Test
    fun `buildProducerProperties should return proper producer properties`() {
        val props = repo.buildProducerProperties()
        assertTrue(props.getProperty(ProducerConfig.CLIENT_ID_CONFIG).equals(KafkaProps.APP_ID))
        assertTrue(props.getProperty(ProducerConfig.ACKS_CONFIG).equals(KafkaProps.ACK_LEVEL))
        assertTrue(props.getProperty(ProducerConfig.RETRIES_CONFIG).equals(KafkaProps.PRODUCER_RETRIES))
        assertTrue(props.getProperty(ProducerConfig.LINGER_MS_CONFIG).equals(KafkaProps.LINGER_DATA))
    }


    @Test
    fun `buildConsumerProperties should return proper consumer properties`() {
        val props = repo.buildConsumerProperties()
        assertTrue(props.getProperty(ConsumerConfig.GROUP_ID_CONFIG).equals(KafkaProps.CONSUMER_GROUP))
        assertTrue(props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equals(KafkaProps.AUTO_COMMIT))
        assertTrue(props.getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG).equals(KafkaProps.COMMIT_INTERVAL_MS))
        assertTrue(props.getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG).equals(KafkaProps.SESSION_TIMEOUT_MS_CONFIG))
        assertTrue(props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).equals(KafkaProps.AUTO_OFFSET_RESET_CONFIG))
        assertTrue(props.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG).equals(KafkaProps.MAX_POLL_RECORDS))
    }

    fun isKafkaOnline() {
        assertTrue(kafkaOnline, "Kafka is not running")
    }

    companion object {

        private val log = LoggerFactory.getLogger(KafkaRepoTest::class.java)

        private var kafkaOnline: Boolean = false

        private lateinit var repo: KafkaRepo
        private lateinit var admin: KafkaAdmin
        private lateinit var kafkaMsg: KafkaMsg
        private lateinit var adminClient: AdminClient
        private val topic = "kafka_repo_test"

        @BeforeAll
        @JvmStatic
        fun setUp() {
            repo = KafkaRepo()
            admin = KafkaAdmin()

            val props = admin.buildAdminProperties()
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000")
            adminClient = KafkaAdminClient.create(props)

            kafkaMsg = KafkaMsg("kafka_repo_test", "1", "test msg")

            isKafkaRunning()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            deleteTopic()
        }

        fun deleteTopic() {
            try {
                adminClient.deleteTopics(Collections.singleton(topic))

            } catch (ex: Exception) {
                log.warn("Failed to delete topic [$topic], ${ex.message}")

            } finally {
                if (this::adminClient.isInitialized) adminClient.close()
            }
        }


        fun isKafkaRunning() {
            try {
                val topics = adminClient.listTopics()
                topics.names().get()
                kafkaOnline = true
            } catch (ex: Exception) {
                log.error("Kafka is not running", ex.cause?.message)
            }
        }
    }
}