package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import io.vertx.core.Vertx
import kotlinx.coroutines.CoroutineScope
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

internal interface Fetcher {

    companion object {
        fun of(
            vertx: Vertx,
            options: FetcherOptions,
            clusterName: OrchestraClusterName,
            startFetchPosition: FetchPosition,
            scope: CoroutineScope,
            shardId: ShardId,
            kinesisClient: KinesisAsyncClient
        ): Fetcher {
            val enhancedOptions = options.enhancedFanOut
            return if (enhancedOptions != null) {
                EnhancedFanoutFetcher(
                    vertx, options, enhancedOptions, clusterName, startFetchPosition.sequenceNumber,
                    scope, shardId, kinesisClient
                )
            } else {
                DynamicRecordFetcher(options, startFetchPosition, scope, clusterName.streamName, shardId, kinesisClient)
            }
        }
    }

    val streamReader: RecordBatchStreamReader
    suspend fun start()
    suspend fun stop()

    fun resetTo(fetchPosition: FetchPosition)
}