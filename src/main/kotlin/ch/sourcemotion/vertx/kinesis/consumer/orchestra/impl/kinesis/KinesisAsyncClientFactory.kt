package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.KinesisClientOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricOptions
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.VertxException
import io.vertx.core.shareddata.Shareable
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.conditions.*
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

/**
 * Factory of Vert.x http client based [KinesisAsyncClient] instances. This factory can be preconfigured, so later [Context] specific instances
 * can be fabricated.
 */
internal class KinesisAsyncClientFactory(
    vertx: Vertx,
    region: String,
    kinesisClientOptions: KinesisClientOptions,
    awsClientMetricOptions: AwsClientMetricOptions? = null
) : AbstractKinesisAsyncClientFactory(vertx, region, kinesisClientOptions, awsClientMetricOptions), Shareable {

    companion object {
        const val SHARED_DATA_REF = "vertx-kinesis-async-client-factory"
        private const val CLIENT_CONTEXT_REF = "vertx-kinesis-client-instance"
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

        val builder = baseBuilderOf(awsCredentialsProvider)
        val retryPolicy = RetryPolicy.defaultRetryPolicy().toBuilder()
            .additionalRetryConditionsAllowed(true)
            .retryCondition(
                // Add VertxException to retry conditions.
                OrRetryCondition.create(
                    RetryOnExceptionsCondition.create(VertxException::class.java),
                    RetryOnStatusCodeCondition.create(SdkDefaultRetrySetting.RETRYABLE_STATUS_CODES),
                    RetryOnExceptionsCondition.create(SdkDefaultRetrySetting.RETRYABLE_EXCEPTIONS),
                    RetryOnClockSkewCondition.create(),
                    RetryOnThrottlingCondition.create()
                )
            ).build()
        builder.overrideConfiguration {
            it.retryPolicy(retryPolicy)
        }

        val client =  VertxSdkClient.withVertx(builder, kinesisClientOptions.kinesisHttpClientOptions, context).build()

        context.put(CLIENT_CONTEXT_REF, client)
        return client
    }
}
