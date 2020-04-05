package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.shareddata.Shareable
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import java.net.URI

/**
 * Factory of [KinesisAsyncClient] instances. This factory can be preconfigured, so later [Context] specific instances
 * can be fabricated.
 */
class KinesisAsyncClientFactory(
    private val vertx: Vertx,
    private val region: String,
    private val kinesisEndpoint: String?
) : Shareable {

    companion object {
        const val SHARED_DATA_REF = "kinesis-async-client-factory"
    }

    fun createKinesisAsyncClient(context: Context): KinesisAsyncClient {
        val awsCredentialsProvider = SharedData.getSharedInstance<ShareableAwsCredentialsProvider>(
            vertx,
            ShareableAwsCredentialsProvider.SHARED_DATA_REF
        )

        val builder = KinesisAsyncClient.builder()
            .region(Region.of(region))
            .credentialsProvider(awsCredentialsProvider)

        kinesisEndpoint?.let { builder.endpointOverride(URI(it)) }

        return VertxSdkClient.withVertx(builder, context).build()
    }
}
