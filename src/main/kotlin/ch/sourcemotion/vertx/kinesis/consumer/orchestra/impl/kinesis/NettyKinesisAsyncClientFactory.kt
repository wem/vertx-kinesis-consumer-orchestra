package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.KinesisClientOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricOptions
import io.reactiverse.awssdk.VertxExecutor
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.shareddata.Shareable
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

/**
 * Factory of AWS Netty http client based [KinesisAsyncClient] instances. This factory can be preconfigured, so later [Context] specific instances
 * can be fabricated.
 */
internal class NettyKinesisAsyncClientFactory(
    vertx: Vertx,
    region: String,
    kinesisClientOptions: KinesisClientOptions,
    awsClientMetricOptions: AwsClientMetricOptions? = null,
    private val sdkNettyMaxConcurrency: Int? = null,
    private val sdkNettyMaxStreams: Long? = null
) : AbstractKinesisAsyncClientFactory(vertx, region, kinesisClientOptions, awsClientMetricOptions), Shareable {

    companion object {
        const val SHARED_DATA_REF = "netty-kinesis-async-client-factory"
        private const val CLIENT_CONTEXT_REF = "netty-kinesis-client-instance"
    }

    fun createKinesisAsyncClient(context: Context): KinesisAsyncClient {
        val existingContextInstance = context.get<KinesisAsyncClient?>(CLIENT_CONTEXT_REF)
        if (existingContextInstance.isNotNull()) {
            return existingContextInstance
        }

        val awsCredentialsProvider = SharedData.getSharedInstance<ShareableAwsCredentialsProvider>(
            vertx,
            ShareableAwsCredentialsProvider.SHARED_DATA_REF
        )

        val httpClient = NettyNioAsyncHttpClient.builder().apply {
            sdkNettyMaxConcurrency?.let { value -> maxConcurrency(value) }
            sdkNettyMaxStreams?.let { value -> http2Configuration { it.maxStreams(value) } }
        }.build()

        val client = baseBuilderOf(awsCredentialsProvider)
            .httpClient(httpClient)
            .asyncConfiguration { conf ->
                conf.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, VertxExecutor(context))
            }.build()

        context.put(CLIENT_CONTEXT_REF, client)
        return client
    }
}
