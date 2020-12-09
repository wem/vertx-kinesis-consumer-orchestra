package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.Record
import java.util.*
import kotlin.random.Random

internal class RecordBatchStreamTest : AbstractVertxTest() {

    private companion object {
        const val PREFETCH_LIMIT = 10
    }

    @Test
    internal fun multiple_writers_will_fail() {
        val stream = RecordBatchStream(PREFETCH_LIMIT)
        stream.writer()
        shouldThrow<UnsupportedOperationException> { stream.writer() }
    }

    @Test
    internal fun writer_suspend_on_pre_fetch_limit_exceeded(testContext: VertxTestContext) = testContext.async {

        val stream = RecordBatchStream(PREFETCH_LIMIT)
        val writer = stream.writer()
        val reader = stream.reader()
        val response = getRecordResponse(recordCount = PREFETCH_LIMIT + 1)
        var continuedAfterWriteToStream = false
        launch {
            writer.writeToStream(response) // Writer should get suspended on this call
            continuedAfterWriteToStream = true
        }
        delay(10)
        continuedAfterWriteToStream.shouldBeFalse()
        reader.readFromStream().verifyAgainstResponse(response, PREFETCH_LIMIT + 1) // Writer should get resumed on this call
        delay(10)
        continuedAfterWriteToStream.shouldBeTrue()
        val responseAfterResume = getRecordResponse(recordCount = 3)
        writer.writeToStream(responseAfterResume)
        reader.readFromStream().verifyAgainstResponse(responseAfterResume, 3)
    }

    @Test
    internal fun reader_suspended_when_no_responses_available(testContext: VertxTestContext) = testContext.async {
        val stream = RecordBatchStream(PREFETCH_LIMIT)
        val writer = stream.writer()
        val reader = stream.reader()

        val response = getRecordResponse(recordCount = 3)
        writer.writeToStream(response)
        val batch = reader.readFromStream()
        batch.verifyAgainstResponse(response, 3)
        var nextResponseAfterEmptyOne: GetRecordsResponse? = null
        launch {
            writer.writeToStream(getRecordResponse(recordCount = 0)) // The reader get suspend on read this response
            delay(100)
            writer.writeToStream(getRecordResponse(recordCount = 0))
            nextResponseAfterEmptyOne = getRecordResponse(recordCount = 1)
            writer.writeToStream(nextResponseAfterEmptyOne!!)
        }
        delay(100)
        nextResponseAfterEmptyOne.shouldNotBeNull().let {
            reader.readFromStream().verifyAgainstResponse(it, 1)
        }
    }

    @Test
    internal fun write_read_write_read(testContext: VertxTestContext) = testContext.async {
        val stream = RecordBatchStream(PREFETCH_LIMIT)
        val writer = stream.writer()
        val reader = stream.reader()
        val expectedBatchSize = 9

        writeResponseReadBatchAndVerify(expectedBatchSize, writer, reader)
        writeResponseReadBatchAndVerify(expectedBatchSize, writer, reader)
    }

    @Test
    internal fun next_shard_iterator_null_first_response(testContext: VertxTestContext) = testContext.async {
        val stream = RecordBatchStream(PREFETCH_LIMIT)
        val writer = stream.writer()
        val reader = stream.reader()
        val response = getRecordResponse(null, Random.nextLong(), 0)
        writer.writeToStream(response)
        val batch = reader.readFromStream()
        batch.nextShardIterator.shouldBeNull()
        batch.records.shouldBeEmpty()
    }

    @Test
    internal fun reset_stream(testContext: VertxTestContext) = testContext.async {
        val stream = RecordBatchStream(PREFETCH_LIMIT)
        val writer = stream.writer()
        val reader = stream.reader()

        writer.writeToStream(getRecordResponse(recordCount = 7))
        writer.resetStream()
        writer.writeToStream(getRecordResponse(recordCount = 3))
        reader.readFromStream().records.shouldHaveSize(3)
    }

    private suspend fun writeResponseReadBatchAndVerify(
        expectedBatchSize: Int,
        writer: RecordBatchStreamWriter,
        reader: RecordBatchStreamReader
    ) {
        val response = getRecordResponse(recordCount = expectedBatchSize)
        writer.writeToStream(response)

        val batch = reader.readFromStream()
        batch.verifyAgainstResponse(response, expectedBatchSize)
    }

    private fun RecordBatch.verifyAgainstResponse(response: GetRecordsResponse, expectedRecordCount: Int) {
        nextShardIterator.shouldBe(ShardIterator(response.nextShardIterator()))
        millisBehindLatest.shouldBe(response.millisBehindLatest())
        sequenceNumber.shouldBe(
            SequenceNumber(
                response.records().last().sequenceNumber(),
                SequenceNumberIteratorPosition.AFTER
            )
        )
        records.verify(expectedRecordCount)
        resharded.shouldBeFalse()
    }

    private fun List<Record>.verify(expectedRecordCount: Int) {
        shouldHaveSize(expectedRecordCount)
        map { it.data() }.shouldContainExactly(IntRange(0, expectedRecordCount - 1).map { recordData(it) })
    }

    private fun nextShardIterator() = ShardIterator("${UUID.randomUUID()}")

    private fun getRecordResponse(
        nextShardIterator: ShardIterator? = nextShardIterator(),
        millisBehindLatest: Long = Random.nextLong(),
        recordCount: Int
    ) =
        getRecordResponse(nextShardIterator, millisBehindLatest, recordList(recordCount))

    private fun getRecordResponse(nextShardIterator: ShardIterator?, millisBehindLatest: Long, records: List<Record>) =
        mock<GetRecordsResponse> {
            on { nextShardIterator() } doReturn if (nextShardIterator != null) "$nextShardIterator" else null
            on { records() } doReturn records
            on { millisBehindLatest() } doReturn if (records.isNotEmpty()) {
                millisBehindLatest
            } else 0
            on { hasRecords() } doReturn records.isNotEmpty()
        }

    private fun recordList(size: Int) = Array(size) {
        record(it)
    }.toList()

    private fun record(idx: Int) = mock<Record> {
        on { data() } doReturn recordData(idx)
        on { sequenceNumber() } doReturn "${UUID.randomUUID()}"
    }

    private fun recordData(idx: Int) = SdkBytes.fromUtf8String("record-$idx")
}


