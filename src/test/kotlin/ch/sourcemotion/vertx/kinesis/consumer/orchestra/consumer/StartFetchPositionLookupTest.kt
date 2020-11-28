package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_CLUSTER_ORCHESTRA_NAME
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer

internal class StartFetchPositionLookupTest : AbstractVertxTest() {
    private companion object {
        const val TEST_STREAM_NAME = "test-stream"
        const val CONSUMER_INFO = "test-consumer-info"
        val shardId = ShardIdGenerator.generateShardId()
    }

    @Test
    internal fun get_start_fetchposition_force_latest(testContext: VertxTestContext) = asyncTest(testContext) {
        val expectedShardIterator = "next-shard-iterator"
        val kinesisClient = mockKinesisClient(expectedShardIterator)
        val sut = StartFetchPositionLookup(
            vertx,
            CONSUMER_INFO,
            shardId,
            createForceLatestShardIteratorOptions(),
            mock(),
            kinesisClient
        )

        val fetchPosition = sut.getStartFetchPosition()

        fetchPosition.iterator.shouldBe(ShardIterator(expectedShardIterator))
        fetchPosition.sequenceNumber.shouldBeNull()
    }

    @Test
    internal fun get_start_fetchposition_existing_or_latest_existing(testContext: VertxTestContext) = asyncTest(testContext) {
        val expectedShardIterator = "next-shard-iterator"
        val expectedSequenceNbrValue = "b99dc866-0fc3-4d36-9cf1-c81d9176c821"
        val expectedSequenceNbrIteratorPosition = SequenceNumberIteratorPosition.AFTER
        val expectedSequenceNumber = SequenceNumber(expectedSequenceNbrValue, expectedSequenceNbrIteratorPosition)

        val kinesisClient = mockKinesisClient(expectedShardIterator)

        val shardStatePersistenceService = mock<ShardStatePersistenceServiceAsync> {
            onBlocking { getConsumerShardSequenceNumber(eq(shardId)) } doReturn expectedSequenceNumber
        }

        val sut = StartFetchPositionLookup(
            vertx,
            CONSUMER_INFO,
            shardId,
            createExistingOrLatestShardIteratorOptions(),
            shardStatePersistenceService,
            kinesisClient,
        )

        val fetchPosition = sut.getStartFetchPosition()

        fetchPosition.iterator.shouldBe(ShardIterator(expectedShardIterator))
        fetchPosition.sequenceNumber.shouldBe(expectedSequenceNumber)
    }

    @Test
    internal fun get_start_fetchposition_existing_or_latest_latest(testContext: VertxTestContext) = asyncTest(testContext) {
        val expectedShardIterator = "next-shard-iterator"

        val kinesisClient = mockKinesisClient(expectedShardIterator)

        val shardStatePersistenceService = mock<ShardStatePersistenceServiceAsync> {
            onBlocking { getConsumerShardSequenceNumber(eq(shardId)) } doReturn null
        }

        val sut = StartFetchPositionLookup(
            vertx,
            CONSUMER_INFO,
            shardId,
            createExistingOrLatestShardIteratorOptions(),
            shardStatePersistenceService,
            kinesisClient,
        )

        val fetchPosition = sut.getStartFetchPosition()

        fetchPosition.iterator.shouldBe(ShardIterator(expectedShardIterator))
        fetchPosition.sequenceNumber.shouldBeNull()
    }

    @Test
    internal fun get_start_fetchposition_existing_or_latest_import(testContext: VertxTestContext) = asyncTest(testContext) {
        val expectedShardIterator = "next-shard-iterator"

        val kinesisClient = mockKinesisClient(expectedShardIterator)

        val shardStatePersistenceService = mock<ShardStatePersistenceServiceAsync> {
            onBlocking { getConsumerShardSequenceNumber(eq(shardId)) } doReturn null
        }

        val sut = StartFetchPositionLookup(
            vertx,
            CONSUMER_INFO,
            shardId,
            createExistingOrLatestShardIteratorOptions(),
            shardStatePersistenceService,
            kinesisClient,
        )

        val fetchPosition = sut.getStartFetchPosition()

        fetchPosition.iterator.shouldBe(ShardIterator(expectedShardIterator))
        fetchPosition.sequenceNumber.shouldBeNull()
    }

    private fun mockKinesisClient(expectedShardIterator: String): KinesisAsyncClient {
        val getResponse = mock<GetShardIteratorResponse> {
            on { shardIterator() } doReturn expectedShardIterator
        }
        return mock {
            on { getShardIterator(any<Consumer<GetShardIteratorRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                getResponse
            )
        }
    }

    private fun createForceLatestShardIteratorOptions() = mock<KinesisConsumerVerticleOptions> {
        on { clusterName } doReturn TEST_CLUSTER_ORCHESTRA_NAME
        on { shardIteratorStrategy } doReturn ShardIteratorStrategy.FORCE_LATEST
    }

    private fun createExistingOrLatestShardIteratorOptions(sequenceNbrImportAddr: String? = null) =
        mock<KinesisConsumerVerticleOptions> {
            on { clusterName } doReturn TEST_CLUSTER_ORCHESTRA_NAME
            on { shardIteratorStrategy } doReturn ShardIteratorStrategy.EXISTING_OR_LATEST
            on { this.sequenceNbrImportAddress } doReturn sequenceNbrImportAddr
        }
}
