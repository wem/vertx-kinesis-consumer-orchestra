package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal abstract class AbstractEnhancedFanoutFetcherTest : AbstractKinesisAndRedisTest(false) {

    private lateinit var sut : EnhancedFanoutFetcher

    @AfterEach
    internal fun tearDown() = asyncBeforeOrAfter {
        sut.stop()
    }

    @Test
    internal fun consume_1000_records(testContext: VertxTestContext) = testContext.async(1000) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        sut = prepareSut(streamDescription)
        sut.start()
        val streamReader = sut.streamReader
        launch {
            while (defaultTestScope.isActive) {
                val batch = streamReader.readFromStream()
                repeat(batch.records.size) {
                    checkpoint.flag()
                }
            }
        }

        sut.awaitIsFetching()

        kinesisClient.putRecords(2 batchesOf 500)
    }

    @Test
    internal fun resharding(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        sut = prepareSut(streamDescription)
        sut.start()
        val streamReader = sut.streamReader

        defaultTestScope.launch {
            while (defaultTestScope.isActive) {
                val batch = streamReader.readFromStream()
                if (batch.records.isNotEmpty()) { // We split shard after first received record

                    testContext.verify { batch.resharded.shouldBeFalse() }
                    kinesisClient.splitShardFair(streamDescription.shards().first())

                    while (defaultTestScope.isActive) {
                        if (streamReader.readFromStream().resharded) {
                            checkpoint.flag()
                        }
                    }
                }
            }
        }

        sut.awaitIsFetching()
        kinesisClient.putRecords(1 batchesOf 1)
    }

    @Test
    internal fun reset(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        sut = prepareSut(streamDescription)
        sut.start()
        val streamReader = sut.streamReader

        var resetSequenceNumber: String? = null
        defaultTestScope.launch {
            while (defaultTestScope.isActive) {
                val batch = streamReader.readFromStream()
                if (batch.records.isNotEmpty() && resetSequenceNumber == null) {
                    val sequenceNumber = batch.records.first().sequenceNumber().also { resetSequenceNumber = it }
                    sut.resetTo(FetchPosition(ShardIterator(sequenceNumber), SequenceNumber(sequenceNumber, SequenceNumberIteratorPosition.AT)))
                }
                if (batch.records.isNotEmpty() && resetSequenceNumber != null) {
                    testContext.verify {
                        batch.records.first().sequenceNumber().shouldBe(resetSequenceNumber)
                        checkpoint.flag()
                    }
                }
            }
        }

        sut.awaitIsFetching()

        vertx.setPeriodic(100) { timerId ->
            defaultTestScope.launch {
                runCatching { kinesisClient.putRecords(1 batchesOf 100) }
                    .onFailure {
                        if (it is ResourceNotFoundException) {
                            vertx.cancelTimer(timerId)
                        }
                    }
            }
        }
    }

    /**
     * Stop means no further fetching, but the reader can still read records.
     * This test covers the proper stop, without any issue.
     */
    @Test
    internal fun stop(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        sut = prepareSut(streamDescription)
        sut.start()
        val streamReader = sut.streamReader

        defaultTestScope.launch {
            while (defaultTestScope.isActive) {
                val batch = streamReader.readFromStream()
                if (batch.records.isNotEmpty()) {
                    sut.stop()
                    checkpoint.flag()
                }
            }
        }

        sut.awaitIsFetching()

        vertx.setPeriodic(100) { timerId ->
            defaultTestScope.launch {
                runCatching { kinesisClient.putRecords(1 batchesOf 100) }
                    .onFailure {
                        if (it is ResourceNotFoundException) {
                            vertx.cancelTimer(timerId)
                        }
                    }
            }
        }
    }

    private suspend fun EnhancedFanoutFetcher.awaitIsFetching() {
        while (fetching.not()){
            delay(100)
        }
    }

    protected abstract suspend fun prepareSut(streamDescription: StreamDescription): EnhancedFanoutFetcher
}