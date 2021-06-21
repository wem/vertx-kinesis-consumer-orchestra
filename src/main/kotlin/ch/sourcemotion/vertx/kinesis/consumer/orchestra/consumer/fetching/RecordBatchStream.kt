package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator
import kotlinx.coroutines.suspendCancellableCoroutine
import software.amazon.awssdk.services.kinesis.model.ChildShard
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent
import java.util.*
import kotlin.coroutines.resume

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

    private var responseEntryQueue = ResponseEntryQueue(recordsPreFetchLimit)

    private var hasWriter = false

    fun writer(): RecordBatchStreamWriter {
        if (hasWriter) {
            throw UnsupportedOperationException("Only 1 writer supported on record batch stream")
        }
        return DefaultRecordBatchStreamWriter(responseEntryQueue).also { hasWriter = true } // Only 1 writer supported
    }

    fun reader() = DefaultRecordBatchStreamReader(responseEntryQueue)
}

internal class DefaultRecordBatchStreamWriter(private val responseEntryChannel: ResponseEntryQueue) :
    RecordBatchStreamWriter {
    override suspend fun writeToStream(response: GetRecordsResponse) {
        val shardIterator = ShardIterator.of(response.nextShardIterator())
        val childShards = response.childShards()
        val entries = if (response.records().isEmpty()) {
            listOf(
                ResponseEntry(null, shardIterator, childShards, response.millisBehindLatest())
            )
        } else {
            response.records().map {
                ResponseEntry(it, shardIterator, childShards, response.millisBehindLatest())
            }
        }
        responseEntryChannel.send(entries)
    }

    override suspend fun writeToStream(event: SubscribeToShardEvent) {
        // On enhanced fan out we have no shard iterator, but sequence number for continuation.
        val shardIterator = ShardIterator.of(event.continuationSequenceNumber())
        val childShards = event.childShards()
        val entries = if (event.records().isEmpty()) {
            listOf(
                ResponseEntry(null, shardIterator, childShards, event.millisBehindLatest() ?: 0)
            )
        } else {
            event.records().map {
                ResponseEntry(it, shardIterator, childShards, event.millisBehindLatest() ?: 0)
            }
        }
        responseEntryChannel.send(entries)
    }
}

internal class DefaultRecordBatchStreamReader(
    private val responseEntryChannel: ResponseEntryQueue
) : RecordBatchStreamReader {
    override suspend fun readFromStream(): RecordBatch {
        val entries = responseEntryChannel.receive()
        val latestEntry = entries.last() // There must be at least one
        val records = entries.mapNotNull { it.record }
        val latestSequenceNumber = if (records.isNotEmpty()) {
            SequenceNumber.after(records.last().sequenceNumber())
        } else null
        return RecordBatch(
            records,
            latestEntry.shardIterator,
            latestEntry.childShards,
            latestSequenceNumber,
            latestEntry.millisBehindLatest
        )
    }

    override fun isEmpty() = responseEntryChannel.isEmpty()
}

internal data class ResponseEntry(
    val record: Record?,
    val shardIterator: ShardIterator?,
    val childShards: List<ChildShard>,
    val millisBehindLatest: Long
)

internal interface RecordBatchStreamWriter {
    suspend fun writeToStream(response: GetRecordsResponse)
    suspend fun writeToStream(event: SubscribeToShardEvent)
}

internal interface RecordBatchStreamReader {
    fun isEmpty(): Boolean
    suspend fun readFromStream(): RecordBatch
}

internal class ResponseEntryQueue(private val limit: Int) {
    private val queueList = LinkedList<ResponseEntry>()

    // This is a list to ensure all senders get resumed, in the case multiple simultaneous calls happen.
    private var sendCont: (() -> Unit)? = null
    private var receiveCont: (() -> Unit)? = null

    suspend fun send(entries: List<ResponseEntry>) {
        entries.forEach { offer(it) }
        resumeReceiver()
    }

    private suspend fun offer(entry: ResponseEntry) {
        if (queueList.size >= limit) {
            resumeReceiver()
            suspendCancellableCoroutine<Unit> {
                sendCont =  {
                    sendCont = null
                    it.resume(Unit)
                }
            }
        }
        queueList.offer(entry)
    }

    suspend fun receive(): List<ResponseEntry> {
        val elementList = ArrayList<ResponseEntry>(queueList.size)
        if (queueList.isEmpty()) {
            resumeSender()
            suspendCancellableCoroutine<Unit> {
                receiveCont = {
                    receiveCont = null
                    it.resume(Unit)
                }
            }
        }
        while (elementList.size < limit && queueList.isNotEmpty()) {
            val element = queueList.poll()
            if (element != null) {
                elementList.add(element)
            }
        }
        resumeSender()
        return elementList
    }

    private fun resumeReceiver() {
        receiveCont?.invoke()
    }

    private fun resumeSender() {
        sendCont?.invoke()
    }

    fun isEmpty() = queueList.isEmpty()
    fun size() = queueList.size
    fun senderSuspended() = sendCont != null
    fun receiverSuspended() = receiveCont != null
}