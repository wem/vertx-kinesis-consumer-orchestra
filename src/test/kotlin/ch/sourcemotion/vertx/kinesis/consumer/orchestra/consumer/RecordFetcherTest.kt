package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import com.nhaarman.mockitokotlin2.*
import io.kotest.matchers.longs.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.CoroutineScope
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import kotlin.test.assertFailsWith

internal class RecordFetcherTest : AbstractVertxTest() {

    private companion object {
        const val STREAM_NAME = "some-stream"
        const val NEXT_ITERATOR = "next-shard-interator"
        val shardId = ShardId("1")

        val defaultFetchPosition = FetchPosition(
            ShardIterator("default-shard-iterator"),
            SequenceNumber("default-sequence-number", SequenceNumberIteratorPosition.AFTER)
        )
    }

    @Test
    internal fun getNextRecords_valid_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        val getRecordsResponse = createGetRecordsResponse()

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                getRecordsResponse
            )
        }

        val sut = createFetcher(kinesisClientMock)

        sut.fetchNextRecords(defaultFetchPosition)
        val response = sut.getNextRecords()
        response.nextShardIterator().shouldBe(NEXT_ITERATOR)
    }

    @Test
    internal fun getNextRecords_expired_iterator(testContext: VertxTestContext) = asyncTest(testContext) {

        val actualShardIterator = "actual-shard-interator"

        val getRecordResponse = createGetRecordsResponse(actualShardIterator)

        val getIteratorResponse: GetShardIteratorResponse = mock {
            on { shardIterator() } doReturn NEXT_ITERATOR
        }

        val kinesisClientMock = mock<KinesisAsyncClient> {
            var firstGetRequestDone = false

            on { getShardIterator(any<Consumer<GetShardIteratorRequest.Builder>>()) } doAnswer {
                CompletableFuture.completedFuture(getIteratorResponse)
            }
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doAnswer {
                // The first get records request fail because of expired shard iterator
                if (firstGetRequestDone.not()) {
                    firstGetRequestDone = true
                    throw ExpiredIteratorException.builder().build()
                } else {
                    CompletableFuture.completedFuture(getRecordResponse)
                }
            }
        }

        val sut = createFetcher(kinesisClientMock)

        sut.fetchNextRecords(defaultFetchPosition)
        val response = sut.getNextRecords()
        response.nextShardIterator().shouldBe(actualShardIterator)
    }

    @Test
    internal fun getNextRecords_should_be_delayed(testContext: VertxTestContext) = asyncTest(testContext) {
        val getRecordsResponse = createGetRecordsResponse()

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                getRecordsResponse
            )
        }

        val delay = 100L

        val sut = createFetcher(kinesisClientMock, delay)

        val start = System.currentTimeMillis()

        sut.fetchNextRecords(defaultFetchPosition)
        sut.getNextRecords()

        val end = System.currentTimeMillis()
        val diff = end - start
        diff.shouldBeGreaterThanOrEqual(delay)
    }

    @Test
    internal fun restart_fetching(testContext: VertxTestContext) = asyncTest(testContext) {
        val restartShardIterator = "restart-shard-iterator"
        val restartSequenceNumber = "restart-sequence-number"

        val getRecordsResponseAfterRestart = createGetRecordsResponse(restartShardIterator)

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doAnswer {
                val getRecordRequestBuilder = GetRecordsRequest.builder()

                it.getArgument<Consumer<GetRecordsRequest.Builder>>(0).accept(getRecordRequestBuilder)

                val req = getRecordRequestBuilder.build()
                val requestedShardIterator = req.shardIterator()

                if (requestedShardIterator == NEXT_ITERATOR) {
                    CompletableFuture.completedFuture(createGetRecordsResponse())
                } else {
                    CompletableFuture.completedFuture(getRecordsResponseAfterRestart)
                }
            }
        }

        val sut = createFetcher(kinesisClientMock)

        sut.fetchNextRecords(defaultFetchPosition)

        // Fetch position that continues after failure. We use here another shard iterator and sequence number just for test.
        // So we are able to verify the fetching continues on the correct position
        val restartFetchPosition = FetchPosition(
            ShardIterator(restartShardIterator),
            SequenceNumber(restartSequenceNumber, SequenceNumberIteratorPosition.AFTER)
        )

        sut.restartFetching(restartFetchPosition)

        val response = sut.getNextRecords()
        response.nextShardIterator().shouldBe(restartShardIterator)
        verify(kinesisClientMock, times(2)).getRecords(any<Consumer<GetRecordsRequest.Builder>>())
    }

    @Test
    internal fun wrong_fetching_access_order(testContext: VertxTestContext) = asyncTest(testContext) {
        val kinesisClient = mock<KinesisAsyncClient>()

        val sut = createFetcher(kinesisClient)
        assertFailsWith<WrongFetchingOrderException> {
            sut.getNextRecords()
        }
    }

    @Test
    internal fun close_fetcher(testContext: VertxTestContext) = asyncTest(testContext) {
        val getRecordsResponse = createGetRecordsResponse()

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                getRecordsResponse
            )
        }

        val sut = createFetcher(kinesisClientMock)
        sut.fetchNextRecords(defaultFetchPosition)
        sut.close()
    }

    private fun createGetRecordsResponse(nextShardIterator: String = NEXT_ITERATOR) = mock<GetRecordsResponse> {
        on { nextShardIterator() } doReturn nextShardIterator
    }

    private fun CoroutineScope.createFetcher(kinesisClient: KinesisAsyncClient, fetchDelay: Long = 0) =
        RecordFetcher(kinesisClient, 1, STREAM_NAME, shardId, this, fetchDelay) { "Test fetcher" }
}
