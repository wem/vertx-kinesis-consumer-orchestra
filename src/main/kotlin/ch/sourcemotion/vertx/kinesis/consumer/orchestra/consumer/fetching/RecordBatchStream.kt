package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.Record

internal data class RecordBatch(
    val records: List<Record>,
    val nextShardIterator: ShardIterator?,
    val sequenceNumber: SequenceNumber?,
    val millisBehindLatest: Long,
) {
    val resharded: Boolean = nextShardIterator == null
}

internal class RecordBatchStream(private val recordsPreFetchLimit: Int) {

    private var responseEntryChannel = Channel<ResponseEntry>(recordsPreFetchLimit)

    private var hasWriter = false

    fun writer(): RecordBatchStreamWriter {
        if (hasWriter) {
            throw UnsupportedOperationException("Only 1 writer supported on record batch stream")
        }
        return object : RecordBatchStreamWriter {
            override suspend fun writeToStream(response: GetRecordsResponse) {
                if (response.records().isEmpty()) {
                    listOf(
                        ResponseEntry(
                            null, ShardIterator.of(response.nextShardIterator()), response.millisBehindLatest()
                        )
                    )
                } else {
                    response.records().map { ResponseEntry(it, ShardIterator.of(response.nextShardIterator()), response.millisBehindLatest()) }
                }.forEach { responseEntryChannel.send(it) }
            }

            override fun resetStream() {
                newChannel()
            }
        }.also { hasWriter = true } // Only 1 writer supported
    }

    fun reader() = object : RecordBatchStreamReader {
        override suspend fun readFromStream(): RecordBatch {
            val entries = ArrayList<ResponseEntry>().apply {
                add(responseEntryChannel.receive())
            }
            while (responseEntryChannel.isEmpty.not()) {
                entries.add(responseEntryChannel.receive())
            }
            val latestEntry = entries.last() // There must be at least one
            val records = entries.mapNotNull { it.record }
            val latestSequenceNumber = if (records.isNotEmpty()) {
                SequenceNumber.after(records.last().sequenceNumber())
            }else null
            return RecordBatch(records, latestEntry.shardIterator, latestSequenceNumber, latestEntry.millisBehindLatest)
        }
    }

    private fun newChannel() {
        val oldRecordChannel = responseEntryChannel
        responseEntryChannel = Channel(recordsPreFetchLimit)
        oldRecordChannel.close()
    }
}

private data class ResponseEntry(val record: Record?, val shardIterator: ShardIterator?, val millisBehindLatest: Long)

internal interface RecordBatchStreamWriter {
    suspend fun writeToStream(response: GetRecordsResponse)
    fun resetStream()
}

internal interface RecordBatchStreamReader {
    suspend fun readFromStream(): RecordBatch
}
