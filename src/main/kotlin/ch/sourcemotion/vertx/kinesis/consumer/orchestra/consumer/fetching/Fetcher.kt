package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.NettyKinesisAsyncClientFactory
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Tags
import io.vertx.core.Vertx
import io.vertx.micrometer.backends.BackendRegistries
import kotlinx.coroutines.CoroutineScope
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

internal interface Fetcher {

    companion object : KLogging(){
        fun of(
            vertx: Vertx,
            fetcherOptions: FetcherOptions,
            clusterName: OrchestraClusterName,
            startFetchPosition: FetchPosition,
            scope: CoroutineScope,
            shardId: ShardId,
            vertxClient: KinesisAsyncClient,
        ): Fetcher {
            val kinesisClient = getFinalClient(vertx, vertxClient)
            val enhancedOptions = fetcherOptions.enhancedFanOut
            val metricsCounter = fetcherOptions.metricsCounterOf(clusterName.streamName, shardId)
            val context = vertx.orCreateContext
            return if (enhancedOptions != null) {
                EnhancedFanoutFetcher(
                    vertx, context, fetcherOptions, enhancedOptions, clusterName, startFetchPosition.sequenceNumber,
                    scope, shardId, kinesisClient, metricsCounter
                )
            } else {
                DynamicRecordFetcher(fetcherOptions, startFetchPosition, scope, clusterName.streamName, shardId, kinesisClient)
            }
        }

        private fun getFinalClient(vertx: Vertx, defaultClient: KinesisAsyncClient) : KinesisAsyncClient {
            return runCatching {
                SharedData.getSharedInstance<NettyKinesisAsyncClientFactory>(vertx, NettyKinesisAsyncClientFactory.SHARED_DATA_REF)
                    .createKinesisAsyncClient(vertx.orCreateContext).also { logger.info { "Using AWS Netty Kinesis client to fetch records" } }
            }.getOrElse { defaultClient.also { logger.info { "Using Vert.x Kinesis client to fetch records" } } }
        }

        private fun FetcherOptions.metricsCounterOf(streamName: String, shardId: ShardId): Counter? = if (metricsEnabled && metricName != null) {
            val registry = if (metricRegistryName != null) {
                BackendRegistries.getNow(metricRegistryName)
            } else {
                BackendRegistries.getDefaultNow()
            }
            val tags = Tags.of("stream", streamName, "shard", "$shardId")
            registry.counter(metricName, tags)
        } else null
    }

    val streamReader: RecordBatchStreamReader
    suspend fun start()
    suspend fun stop()

    fun resetTo(fetchPosition: FetchPosition)
}