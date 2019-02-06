package com.pakybytes.demo.kafkacli.delivery

import com.pakybytes.demo.kafkacli.di.ServiceLocator
import org.slf4j.LoggerFactory
import java.util.concurrent.*
import java.util.function.Supplier


object Main {

    private val log = LoggerFactory.getLogger(Main::class.java)

    private val executerService = Executors.newCachedThreadPool()
    private val schedulerService = Executors.newScheduledThreadPool(1)

    private const val TOPIC = "topic-1"
    private const val LIFECYLCE_TIMEOUT = 10000L


    @JvmStatic
    fun main(args: Array<String>) {

        // Get Services
        val producerService = ServiceLocator.get().provideProducerService()
        val consumerService = ServiceLocator.get().provideConsumerService()

        runFor(
                Supplier {
                    producerService.startSending(
                            producerService.buildProducer(),
                            TOPIC)
                },
                Supplier {
                    consumerService.startReading(
                            consumerService.buildConsumer(),
                            TOPIC)
                },
                LIFECYLCE_TIMEOUT
        )

        Thread.sleep(LIFECYLCE_TIMEOUT + 1000)
        executerService.shutdown()
        schedulerService.shutdown()
    }


    fun <T> runFor(producer: Supplier<T>, consumer: Supplier<T>, timeoutMs: Long) {

        val (producerFuture, producerCF) = startTask(producer)

        val (consumerFuture, consumerCF) = startTask(consumer)

        schedulerService.schedule(
                {
                    if (!producerCF.isDone) {
                        log.warn("canceling producer task")
                        producerCF.cancel(true)
                        producerFuture.cancel(true)
                    }
                    if (!consumerCF.isDone) {
                        log.warn("canceling consumer task")
                        consumerCF.cancel(true)
                        consumerFuture.cancel(true)
                    }
                }, timeoutMs, TimeUnit.MILLISECONDS)
    }


    fun <T> startTask(supplier: Supplier<T>): Pair<Future<*>, CompletableFuture<T>> {

        val cf = CompletableFuture<T>()

        val future = executerService.submit {
            try {
                cf.complete(supplier.get())
            } catch (ex: Exception) {
                cf.completeExceptionally(ex)
            }
        }

        return Pair(future, cf)
    }

}
