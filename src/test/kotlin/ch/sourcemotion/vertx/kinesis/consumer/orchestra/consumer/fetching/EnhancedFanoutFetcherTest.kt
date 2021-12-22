package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_APPLICATION_NAME
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_STREAM_NAME
import com.nhaarman.mockitokotlin2.KStubbing
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.mock
import io.kotest.assertions.throwables.shouldThrow
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*
import java.util.concurrent.CompletableFuture

internal class EnhancedFanoutFetcherTest : AbstractVertxTest() {

    private companion object {
        const val STREAM_ARN = "stream-arn"
        const val CONSUMER_ARN = "consumer-arn"
        val clusterName = OrchestraClusterName(TEST_APPLICATION_NAME, TEST_STREAM_NAME)
        val shardId = ShardIdGenerator.generateShardId(0)
        val consumerName = clusterName.applicationName

        val enhancedFanOutOptions = EnhancedFanOutOptions(STREAM_ARN)

        val defaultFetcherOptions = FetcherOptions(enhancedFanOut = enhancedFanOutOptions)
        val defaultStartSequenceNumber =
            SequenceNumber("54c63f45-5d71-4834-99b1-40c13967434c", SequenceNumberIteratorPosition.AFTER)
    }

    @Test
    internal fun no_registration_if_consumer_exist(testContext: VertxTestContext) = testContext.async(2) { checkpoint ->
        val consumer = activeConsumer()
        val kinesisClient = mock<KinesisAsyncClient> {
            describeConsumer(consumer, checkpoint = checkpoint)
            listConsumers(consumer)

            on { registerStreamConsumer(any<java.util.function.Consumer<RegisterStreamConsumerRequest.Builder>>()) } doAnswer {
                val cause = Exception("Consumer found, no registration should happen")
                testContext.failNow(cause)
                CompletableFuture.failedFuture(cause)
            }

            on { subscribeToShard(any<SubscribeToShardRequest>(), any()) } doAnswer {
                checkpoint.flag()
                CompletableFuture.completedFuture(null)
            }
        }

        val sut = defaultEnhancedFanOutFetcher()
        sut.start()
    }

    @Test
    internal fun consumer_registration_if_not_exist(testContext: VertxTestContext) =
        testContext.async(3) { checkpoint ->
            val consumer = activeConsumer()
            val kinesisClient = mock<KinesisAsyncClient> {
                listConsumers(emptyList())
                describeConsumer(consumer, checkpoint = checkpoint)

                on { registerStreamConsumer(any<java.util.function.Consumer<RegisterStreamConsumerRequest.Builder>>()) } doAnswer {
                    checkpoint.flag()
                    CompletableFuture.completedFuture(
                        RegisterStreamConsumerResponse.builder().consumer(consumer).build()
                    )
                }

                on { subscribeToShard(any<SubscribeToShardRequest>(), any()) } doAnswer {
                    checkpoint.flag()
                    CompletableFuture.completedFuture(null)
                }
            }

            val sut = defaultEnhancedFanOutFetcher()
            sut.start()
        }

    @Test
    internal fun consumer_becomes_active(testContext: VertxTestContext) = testContext.async(2) { checkpoint ->
        val creatingConsumer = creatingConsumer()
        val kinesisClient = mock<KinesisAsyncClient> {
            listConsumers(creatingConsumer())
            describeConsumer(creatingConsumer, true, checkpoint)

            on { registerStreamConsumer(any<java.util.function.Consumer<RegisterStreamConsumerRequest.Builder>>()) } doAnswer {
                val cause = Exception("Consumer found, no registration should happen")
                testContext.failNow(cause)
                CompletableFuture.failedFuture(cause)
            }

            on { subscribeToShard(any<SubscribeToShardRequest>(), any()) } doAnswer {
                checkpoint.flag()
                CompletableFuture.completedFuture(null)
            }
        }

        val sut = defaultEnhancedFanOutFetcher()
        sut.start()
    }

    @Test
    internal fun start_failure_because_list_consumer_failed(testContext: VertxTestContext) = testContext.async {
        val kinesisClient = mock<KinesisAsyncClient> {
            on { listStreamConsumers(any<java.util.function.Consumer<ListStreamConsumersRequest.Builder>>()) } doAnswer {
                CompletableFuture.failedFuture(Exception("Test failure"))
            }
        }

        val sut = defaultEnhancedFanOutFetcher()
        shouldThrow<VertxKinesisConsumerOrchestraException> { sut.start() }
    }


    private fun KStubbing<KinesisAsyncClient>.listConsumers(consumers: List<Consumer>) {
        on { listStreamConsumers(any<java.util.function.Consumer<ListStreamConsumersRequest.Builder>>()) } doAnswer {
            CompletableFuture.completedFuture(
                ListStreamConsumersResponse.builder().consumers(consumers).build()
            )
        }
    }

    private fun KStubbing<KinesisAsyncClient>.listConsumers(consumer: Consumer) = listConsumers(listOf(consumer))

    private fun KStubbing<KinesisAsyncClient>.describeConsumer(consumer: Consumer, describeAsActive: Boolean = false, checkpoint: Checkpoint? = null) {
        on { describeStreamConsumer(any<java.util.function.Consumer<DescribeStreamConsumerRequest.Builder>>()) } doAnswer {
            checkpoint?.flag()
            val consumerToDescribe = if (describeAsActive) {
                consumer.activate()
            } else consumer
            CompletableFuture.completedFuture(describeConsumerResponseOf(consumerToDescribe))
        }
    }

    private fun describeConsumerResponseOf(consumer: Consumer) =
        DescribeStreamConsumerResponse.builder().consumerDescription(
            ConsumerDescription.builder()
                .consumerARN(consumer.consumerARN()).consumerName(consumer.consumerName())
                .consumerStatus(consumer.consumerStatus()).build()
        ).build()

    private fun defaultEnhancedFanOutFetcher() =
        EnhancedFanoutFetcher(defaultFetcherOptions)

    private fun activeConsumer() = Consumer.builder().consumerARN(CONSUMER_ARN).consumerName(
        consumerName
    ).consumerStatus(ConsumerStatus.ACTIVE).build()

    private fun creatingConsumer() = Consumer.builder().consumerARN(CONSUMER_ARN).consumerName(
        consumerName
    ).consumerStatus(ConsumerStatus.CREATING).build()

    private fun Consumer.activate() = toBuilder().consumerStatus(ConsumerStatus.ACTIVE).build()
}