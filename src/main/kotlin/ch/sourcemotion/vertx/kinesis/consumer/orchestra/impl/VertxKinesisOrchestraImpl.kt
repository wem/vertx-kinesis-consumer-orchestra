package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestra
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.OrchestraCodecs
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistenceFactory
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deployVerticleAwait
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

class VertxKinesisOrchestraImpl(
    private val vertx: Vertx,
    private val options: VertxKinesisOrchestraOptions
) : VertxKinesisOrchestra {

    suspend fun startOrchestration() {
        OrchestraCodecs.deployCodecs(vertx.eventBus())

        val awsCredentialsProvider = options.credentialsProviderSupplier.get()
        shareCredentials(awsCredentialsProvider)
        shareFactories(vertx)

        val check = JsonObject.mapFrom(options.asOrchestraVerticleOptions())
        check.mapTo(OrchestrationVerticleOptions::class.java)

        val orchesterDeploymentId =
            runCatching {
                vertx.deployVerticleAwait(
                    OrchestrationVerticle::class.java.name,
                    DeploymentOptions().setConfig(JsonObject.mapFrom(options.asOrchestraVerticleOptions()))
                )
            }.getOrElse {
                throw VertxKinesisConsumerOrchestraException(
                    "Unable to start Kinesis consumer orchestra",
                    it
                )
            }
        vertx.orCreateContext.addCloseHook {
            vertx.undeploy(orchesterDeploymentId)
        }
    }

    private fun shareCredentials(credentialsProvider: AwsCredentialsProvider) {
        val shareableAwsCredentialsProvider = ShareableAwsCredentialsProvider(credentialsProvider)
        SharedData.shareInstance(
            vertx,
            shareableAwsCredentialsProvider,
            ShareableAwsCredentialsProvider.SHARED_DATA_REF
        )
    }

    /**
     * Sharing of some factories like Kinesis async client, so we have a single point of configuration.
     */
    private fun shareFactories(vertx: Vertx) {
        SharedData.shareInstance(
            vertx,
            KinesisAsyncClientFactory(vertx, options.region, options.kinesisEndpoint),
            KinesisAsyncClientFactory.SHARED_DATA_REF
        )

        SharedData.shareInstance(
            vertx,
            ShardStatePersistenceFactory(
                options.shardProgressExpiration,
                RedisKeyFactory(options.applicationName, options.streamName)
            ),
            ShardStatePersistenceFactory.SHARED_DATA_REF
        )
    }
}

