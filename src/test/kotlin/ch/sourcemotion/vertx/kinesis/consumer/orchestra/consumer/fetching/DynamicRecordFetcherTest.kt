package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.*
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import java.util.function.Supplier

internal class DynamicRecordFetcherTest : AbstractVertxTest() {

    private companion object {
        const val STREAM = "stream"
        val shardId = ShardIdGenerator.generateShardId(1)
        val dummyFetchPosition = FetchPosition(
            ShardIterator("bf69f888-c9c3-4b2a-9156-da3b9228b18b"),
            SequenceNumber("06cd1487-8529-4d96-aa99-17016691f17b", SequenceNumberIteratorPosition.AFTER)
        )
    }

    @Test
    internal fun sunny_case(testContext: VertxTestContext) {
        val recordsPerResponse = 100
        val responseCount = 100
        testContext.async(recordsPerResponse * responseCount) { checkpoint ->
            val responses = IntRange(1, responseCount).map { getRecordsResponse(recordList(recordsPerResponse)) }
            val kinesisClient = kinesisClient(responses)
            val sut = DynamicRecordFetcher(
                FetcherOptions(recordsFetchIntervalMillis = 1L),
                dummyFetchPosition,
                this,
                STREAM,
                shardId,
                kinesisClient
            )
            sut.start()
            val streamReader = sut.streamReader

            while (true) {
                val batch = streamReader.readFromStream()
                batch.records.forEach { _ ->
                    checkpoint.flag()
                }
            }
        }
    }

    @Test
    internal fun reset(testContext: VertxTestContext) = testContext.async {
        var reset = false
        val kinesisClient = kinesisClient {
            if (reset) {
                getRecordsResponse(recordList(1), dummyFetchPosition.iterator)
            } else getRecordsResponse(recordList(1))
        }
        val sut = DynamicRecordFetcher(FetcherOptions(), dummyFetchPosition, this, STREAM, shardId, kinesisClient)
        sut.start()
        sut.resetTo(dummyFetchPosition)
        reset = true
        sut.streamReader.readFromStream().nextShardIterator.shouldBe(dummyFetchPosition.iterator)
    }


    @Test
    internal fun provisioned_through_put_exceeded(testContext: VertxTestContext) {
        val expectedRecordsCount = 1000
        testContext.async(expectedRecordsCount + (expectedRecordsCount / 2)) { checkpoint ->
            var requestNbr = 0
            val kinesisClient = kinesisClient {
                ++requestNbr
                if (requestNbr % 2 == 0) { // Each second request will throw an exception
                    checkpoint.flag()
                    throw ProvisionedThroughputExceededException.builder().build()
                } else {
                    getRecordsResponse(recordList(10))
                }
            }

            val sut = DynamicRecordFetcher(
                FetcherOptions(recordsFetchIntervalMillis = 1L),
                dummyFetchPosition,
                this,
                STREAM,
                shardId,
                kinesisClient
            )
            sut.start()
            val streamReader = sut.streamReader
            while (true) {
                val batch = streamReader.readFromStream()
                batch.records.forEach { _ ->
                    checkpoint.flag()
                }
            }
        }
    }

    @Test
    internal fun expired_iterator(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val kinesisClient = mock<KinesisAsyncClient> {
            on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.failedFuture(
                ExpiredIteratorException.builder().build()
            )
            on { getShardIterator(any<Consumer<GetShardIteratorRequest.Builder>>()) } doAnswer {
                checkpoint.flag()
                CompletableFuture.completedFuture(mock { on { shardIterator() } doReturn "${UUID.randomUUID()}" })
            }
        }
        DynamicRecordFetcher(FetcherOptions(), dummyFetchPosition, this, STREAM, shardId, kinesisClient).start()
    }

    @Test
    internal fun stop_fetcher(testContext: VertxTestContext) = testContext.async {
        val sut = DynamicRecordFetcher(
            FetcherOptions(),
            dummyFetchPosition,
            this,
            STREAM,
            shardId,
            kinesisClient {
                getRecordsResponse(recordList(100))
            }
        )
        sut.start()
        delay(100) // We give the fetcher time to start, as it runs in another coroutine
        sut.stop()
        sut.running.shouldBeFalse()
    }

    @Test
    internal fun shard_iterator_null(testContext: VertxTestContext) = testContext.async {
        val recordCount = 77
        val sut = DynamicRecordFetcher(
            FetcherOptions(),
            dummyFetchPosition,
            this,
            STREAM,
            shardId,
            kinesisClient {
                getRecordsResponse(recordList(recordCount), shardIterator = null)
            }
        )

        sut.start()
        val streamReader = sut.streamReader
        var consumedRecords = 0
        while (true) {
            val batch = streamReader.readFromStream()
            consumedRecords += batch.records.size
            if (consumedRecords == recordCount) {
                batch.nextShardIterator.shouldBeNull()
                sut.running.shouldBeFalse()
                break
            }
        }
    }

    private fun kinesisClient(supplier: Supplier<GetRecordsResponse>) = mock<KinesisAsyncClient> {
        on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doAnswer {
            CompletableFuture.supplyAsync(supplier)
        }
    }

    private fun kinesisClient(responses: List<GetRecordsResponse>) = mock<KinesisAsyncClient> {
        val responseIter = responses.iterator()
        on { getRecords(any<Consumer<GetRecordsRequest.Builder>>()) } doReturn CompletableFuture.completedFuture(
            if (responseIter.hasNext()) {
                responseIter.next()
            } else emptyRetRecordsResponse()
        )
    }

    private fun emptyRetRecordsResponse() = mock<GetRecordsResponse> {
        on { records() } doReturn emptyList()
        on { hasRecords() } doReturn false
        on { nextShardIterator() } doReturn "${UUID.randomUUID()}"
    }

    private fun getRecordsResponse(
        records: List<Record>,
        shardIterator: ShardIterator? = ShardIterator("${UUID.randomUUID()}")
    ) = mock<GetRecordsResponse> {
        on { records() } doReturn records
        on { hasRecords() } doReturn records.isNotEmpty()
        on { nextShardIterator() } doReturn shardIterator?.let { "$it" }
    }

    private fun recordList(recordCount: Int) = IntRange(1, recordCount).map { recordNbr ->
        mock<Record> {
            on { data() } doReturn SdkBytes.fromUtf8String("record-$recordNbr")
            on { sequenceNumber() } doReturn "${UUID.randomUUID()}"
        }
    }
}
