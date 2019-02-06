package com.pakybytes.demo.kafkacli.di

import com.pakybytes.demo.kafkacli.core.services.ConsumerService
import com.pakybytes.demo.kafkacli.core.services.ProducerService
import org.slf4j.LoggerFactory


/**
 * This class is an alternative to DI
 */
class ServiceLocator {

    private val log = LoggerFactory.getLogger(this::class.java)

    init {
        log.debug("Starting Service Locator [$this]...")
    }


    lateinit var producerService: ProducerService

    lateinit var consumerService: ConsumerService


    fun provideProducerService(): ProducerService =
            if (this::producerService.isInitialized) producerService
            else {
                this.producerService = ProducerService()
                this.producerService
            }

    fun provideConsumerService(): ConsumerService =
            if (this::consumerService.isInitialized) consumerService
            else {
                this.consumerService = ConsumerService()
                this.consumerService
            }


    companion object {

        private lateinit var serviceLocator: ServiceLocator

        fun get(): ServiceLocator =
                if (this::serviceLocator.isInitialized) {
                    serviceLocator
                } else {
                    serviceLocator = ServiceLocator()
                    serviceLocator
                }
    }
}