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
        const val ITERATOR = "next-interator"
        val shardId = ShardId("1")

        val queryPosition = QueryPosition(
            ShardIterator("shard-iterator"),
            SequenceNumber("sequence-number", SequenceNumberIteratorPosition.AFTER)
        )
    }

    @Test
    internal fun getNextRecords_valid_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        val responseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn ITERATOR
        }
        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                responseMock
            )
        }

        val sut = createFetcher(kinesisClientMock)

        sut.fetchNextRecords(queryPosition)
        val response = sut.getNextRecords()
        response.nextShardIterator().shouldBe(ITERATOR)
    }

    @Test
    internal fun getNextRecords_expired_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        var exceptionThrown = false

        val getRecordResponseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn ITERATOR
        }

        val getIteratorResponseMock: GetShardIteratorResponse = mock {
            on { shardIterator() } doReturn ITERATOR
        }

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getShardIterator(any<Consumer<GetShardIteratorRequest.Builder>>()) } doAnswer {
                CompletableFuture.completedFuture(getIteratorResponseMock)
            }
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doAnswer {
                // The first get records request fail because of expired shard iterator
                if (exceptionThrown.not()) {
                    exceptionThrown = true
                    throw ExpiredIteratorException.builder().build()
                } else {
                    CompletableFuture.completedFuture(
                        getRecordResponseMock
                    )
                }
            }
        }

        val sut = createFetcher(kinesisClientMock)

        sut.fetchNextRecords(queryPosition)
        val response = sut.getNextRecords()
        response.nextShardIterator().shouldBe(ITERATOR)
    }

    @Test
    internal fun getNextRecords_should_be_delayed(testContext: VertxTestContext) = asyncTest(testContext) {
        val responseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn ITERATOR
        }

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                responseMock
            )
        }

        val delay = 100L

        val sut = createFetcher(kinesisClientMock, delay)

        val start = System.currentTimeMillis()

        sut.fetchNextRecords(queryPosition)
        sut.getNextRecords()

        val end = System.currentTimeMillis()
        val diff = end - start
        diff.shouldBeGreaterThanOrEqual(delay)
    }

    @Test
    internal fun exception_case_restart_fetching(testContext: VertxTestContext) = asyncTest(testContext) {
        val responseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn ITERATOR
        }

        val previousShardIterator = "previous-shard-iterator"
        val previousShardIteratorResponseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn previousShardIterator
        }

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doAnswer {
                val builder = GetRecordsRequest.builder()
                (it.arguments.first() as Consumer<GetRecordsRequest.Builder>).accept(builder)
                val req = builder.build()
                val requestedShardIterator = req.shardIterator()
                if (requestedShardIterator == ITERATOR) {
                    CompletableFuture.completedFuture(responseMock)
                } else {
                    CompletableFuture.completedFuture(previousShardIteratorResponseMock)
                }
            }
        }

        val sut = createFetcher(kinesisClientMock)

        sut.fetchNextRecords(queryPosition)

        val previousPosition = QueryPosition(
            ShardIterator(previousShardIterator),
            SequenceNumber("sequence-number", SequenceNumberIteratorPosition.AFTER)
        )

        sut.restartFetching(previousPosition)

        val response = sut.getNextRecords()
        response.nextShardIterator().shouldBe(previousShardIterator)
        verify(kinesisClientMock, times(2)).getRecords(any<Consumer<GetRecordsRequest.Builder>>())
    }

    @Test
    internal fun wrong_fetching_access_order(testContext: VertxTestContext) = asyncTest(testContext) {
        val kinesisClientMock = mock<KinesisAsyncClient> { }
        val sut = createFetcher(kinesisClientMock)
        assertFailsWith<RecordFetcher.WrongFetchingOrderException> {
            sut.getNextRecords()
        }
    }

    @Test
    internal fun close_fetcher(testContext: VertxTestContext) = asyncTest(testContext) {
        val responseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn ITERATOR
        }

        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                responseMock
            )
        }

        val sut = createFetcher(kinesisClientMock)
        sut.fetchNextRecords(queryPosition)
        sut.close()
    }

    private fun CoroutineScope.createFetcher(kinesisClient: KinesisAsyncClient, fetchDelay: Long = 0) =
        RecordFetcher(kinesisClient, 1, STREAM_NAME, shardId, this, fetchDelay) { "Test fetcher" }
}
