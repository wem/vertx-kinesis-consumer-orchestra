package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.util.concurrent.TimeUnit

internal abstract class AbstractEnhancedFanoutFetcherTest : AbstractKinesisAndRedisTest(false) {

    private lateinit var sut: EnhancedFanoutFetcher

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

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun resharding(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        sut = prepareSut(streamDescription)
        sut.start()
        val streamReader = sut.streamReader
        var resharded = false

        defaultTestScope.launch {
            while (defaultTestScope.isActive) {
                val batch = streamReader.readFromStream()
                if (batch.records.isNotEmpty()) { // We split shard after first received record
                    if (!resharded) {
                        resharded = true
                        testContext.verify { batch.resharded.shouldBeFalse() }
                        kinesisClient.splitShardFair(streamDescription.shards().first())
                    }
                }
                if (streamReader.readFromStream().resharded) {
                    testContext.verify { resharded.shouldBeTrue() }
                    checkpoint.flag()
                }
            }
        }

        sut.awaitIsFetching()
        kinesisClient.putRecords(1 batchesOf 1)
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
        while (fetching.not()) {
            delay(100)
        }
    }

    protected abstract suspend fun prepareSut(streamDescription: StreamDescription): EnhancedFanoutFetcher
}