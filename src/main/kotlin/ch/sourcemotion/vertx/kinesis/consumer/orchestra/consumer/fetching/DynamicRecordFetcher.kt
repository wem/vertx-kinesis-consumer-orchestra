package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException

internal class DynamicRecordFetcher(
    options: FetcherOptions,
    startingPosition: FetchPosition,
    scope: CoroutineScope,
    private val streamName: String,
    private val shardId: ShardId,
    private val kinesis: KinesisAsyncClient
) {
    private companion object : KLogging()

    private val stream = RecordBatchStream(options.recordsPreFetchLimit)
    private val streamWriter = stream.writer()
    val streamReader = stream.reader()

    private val dynamicLimit = options.dynamicLimitAdjustment.enabled
    private val recordsFetchInterval = options.recordsFetchIntervalMillis
    private val job = scope.launch(start = CoroutineStart.LAZY) { fetch() }
    var running = false
        private set
    private val limitAdjustment = GetRecordsLimitAdjustment.withOptions(streamName, shardId, options)

    private var currentPosition = startingPosition
    private var skipNextResponse = false


    fun start() = this.also {
        logger.info { "Dynamic limit to consume records from Kinesis enabled = \"$dynamicLimit\"" }
        running = true
        job.start()
    }

    suspend fun stop() {
        running = false
        job.join()
    }

    fun resetTo(fetchPosition: FetchPosition) {
        streamWriter.resetStream()
        skipNextResponse = true
        currentPosition = fetchPosition
        logger.info { "Record fetcher reset on stream \"$streamName\" / shard \"$shardId\"" }
    }

    private suspend fun fetch() {
        while (running) {
            skipNextResponse = false // If the reset did happen before here, it's not relevant.
            val (response, duration) = getRecords()
            if (response.isNotNull() && !skipNextResponse) { // On reset from another coroutine the request can be inflight (this coroutine is suspended) but the response must be ignored
                val currentSequenceNumber = if (response.records().isNotEmpty()) {
                    SequenceNumber(response.records().last().sequenceNumber(), SequenceNumberIteratorPosition.AFTER)
                } else currentPosition.sequenceNumber
                if (response.nextShardIterator() != null) {
                    currentPosition = FetchPosition(ShardIterator(response.nextShardIterator()), currentSequenceNumber)
                } else {
                    running = false
                    logger.info { "Record fetcher stopped on stream \"$streamName\" / shard $shardId because shard iterator did end" }
                }
                if (dynamicLimit) {
                    limitAdjustment.includeResponse(response)
                }
                streamWriter.writeToStream(response)
            }
            val delayUntilNextRequest = recordsFetchInterval - duration
            if (delayUntilNextRequest > 0) {
                delay(delayUntilNextRequest)
            }
        }
    }

    private suspend fun getRecords(): Pair<GetRecordsResponse?, Long> {
        val startTime = System.currentTimeMillis()
        return kinesis.getRecords {
            it.limit(limitAdjustment.calculateNextLimit()).shardIterator("${currentPosition.iterator}")
        }.runCatching {
            val response = await()
            Pair(response, System.currentTimeMillis() - startTime)
        }.getOrElse {
            when (it) {
                is ProvisionedThroughputExceededException -> logger.debug(it) { "Too aggressive fetching" }
                is ExpiredIteratorException -> onShardIteratorExpired()
                else -> logger.warn(it) { "Get records request did fail" }
            }
            Pair(null, System.currentTimeMillis() - startTime)
        }
    }

    private suspend fun onShardIteratorExpired(): ShardIterator {
        val sequenceNumber = currentPosition.sequenceNumber
        if (sequenceNumber != null) {
            val iterator =
                kinesis.runCatching { getShardIteratorBySequenceNumberAwait(streamName, shardId, sequenceNumber) }
                    .getOrElse {
                        logger.warn(it) { "get shard iterator by sequence number did fail. We retry" }
                        onShardIteratorExpired()
                    }
            currentPosition = FetchPosition(iterator, sequenceNumber)
            logger.info { "Use fresh shard iterator because previous one did expire" }
            return iterator
        } else {
            running = false
            throw IllegalStateException(
                "Shard iterator expired on stream \"$streamName\" / shard \"$shardId\" and no sequence" +
                        " number available. So we cannot continue fetching. This is a fatal case and should never happen."
            )
        }
    }
}
