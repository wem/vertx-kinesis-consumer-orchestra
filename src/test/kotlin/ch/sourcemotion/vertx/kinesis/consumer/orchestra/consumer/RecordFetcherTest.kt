package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer

internal class RecordFetcherTest : AbstractVertxTest() {

    private companion object {
        val queryPosition = QueryPosition(
            ShardIterator("previous-shard-iterator"),
            SequenceNumber("sequence-number", SequenceNumberIteratorPosition.AFTER)
        )
    }

    @Test
    internal fun getNextRecords_valid_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        val nextIterator = "next-interator"
        val responseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn nextIterator
        }
        val kinesisClientMock = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
                responseMock
            )
        }

        val sut = RecordFetcher(kinesisClientMock, 1, "some-stream", ShardId("1")) { "" }

        val response = sut.fetchNextRecords(queryPosition)
        response.nextShardIterator().shouldBe(nextIterator)
    }

    @Test
    internal fun getNextRecords_expired_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        val nextIterator = "next-interator"
        var exceptionThrown = false

        val getRecordResponseMock: GetRecordsResponse = mock {
            on { nextShardIterator() } doReturn nextIterator
        }

        val getIteratorResponseMock: GetShardIteratorResponse = mock {
            on { shardIterator() } doReturn nextIterator
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

        val sut = RecordFetcher(kinesisClientMock, 1, "some-stream", ShardId("1")) { "" }

        val response = sut.fetchNextRecords(queryPosition)
        response.nextShardIterator().shouldBe(nextIterator)
    }
}
