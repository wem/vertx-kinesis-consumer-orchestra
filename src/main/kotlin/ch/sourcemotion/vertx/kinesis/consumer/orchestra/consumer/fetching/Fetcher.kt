package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.FetchPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Tags
import io.reactiverse.awssdk.VertxExecutor
import io.vertx.core.Vertx
import io.vertx.micrometer.backends.BackendRegistries
import kotlinx.coroutines.CoroutineScope
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
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
            val metricsCounter = options.metricsCounterOf(clusterName.streamName, shardId)
            return if (enhancedOptions != null) {
                val awsCredentialsProvider = SharedData.getSharedInstance<ShareableAwsCredentialsProvider>(
                    vertx,
                    ShareableAwsCredentialsProvider.SHARED_DATA_REF
                )

                val context = vertx.orCreateContext

                val enhancedClient = KinesisAsyncClient.builder()
                    .region(Region.EU_WEST_1)
                    .credentialsProvider(awsCredentialsProvider)
                    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(100).build())
                    .asyncConfiguration { conf ->
                        conf.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, VertxExecutor(context))
                    }.build()

                EnhancedFanoutFetcher(
                    vertx, context, options, enhancedOptions, clusterName, startFetchPosition.sequenceNumber,
                    scope, shardId, enhancedClient, metricsCounter
                )
            } else {
                DynamicRecordFetcher(options, startFetchPosition, scope, clusterName.streamName, shardId, kinesisClient)
            }
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