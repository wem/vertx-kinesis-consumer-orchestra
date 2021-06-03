package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import kotlinx.coroutines.channels.Channel
import software.amazon.awssdk.services.kinesis.model.ChildShard
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent

internal data class RecordBatch(
    val records: List<Record>,
    val nextShardIterator: ShardIterator?,
    val childShards: List<ChildShard>,
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
                val shardIterator = ShardIterator.of(response.nextShardIterator())
                val childShards = response.childShards()
                if (response.records().isEmpty()) {
                    listOf(
                        ResponseEntry(
                            null, shardIterator, childShards, response.millisBehindLatest()
                        )
                    )
                } else {
                    response.records().map {
                        ResponseEntry(it, shardIterator, childShards, response.millisBehindLatest())
                    }
                }.forEach { responseEntryChannel.send(it) }
            }

            override suspend fun writeToStream(event: SubscribeToShardEvent) {
                // On enhanced fan out we have no shard iterator, but sequence number for continuation.
                val shardIterator = ShardIterator.of(event.continuationSequenceNumber())
                val childShards = event.childShards()
                if (event.records().isEmpty()) {
                    listOf(
                        ResponseEntry(
                            null, shardIterator, childShards, event.millisBehindLatest() ?: 0
                        )
                    )
                } else {
                    event.records().map {
                        ResponseEntry(
                            it, shardIterator, childShards, event.millisBehindLatest() ?: 0
                        )
                    }
                }.forEach {
                    responseEntryChannel.send(it)
                }
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
            } else null
            return RecordBatch(records, latestEntry.shardIterator, latestEntry.childShards, latestSequenceNumber, latestEntry.millisBehindLatest)
        }

        override fun isEmpty() = responseEntryChannel.isEmpty
    }

    private fun newChannel() {
        val oldRecordChannel = responseEntryChannel
        responseEntryChannel = Channel(recordsPreFetchLimit)
        oldRecordChannel.close()
    }
}

private data class ResponseEntry(
    val record: Record?,
    val shardIterator: ShardIterator?,
    val childShards: List<ChildShard>,
    val millisBehindLatest: Long
)

internal interface RecordBatchStreamWriter {
    suspend fun writeToStream(response: GetRecordsResponse)
    suspend fun writeToStream(event: SubscribeToShardEvent)
    fun resetStream()
}

internal interface RecordBatchStreamReader {
    fun isEmpty(): Boolean
    suspend fun readFromStream(): RecordBatch
}
