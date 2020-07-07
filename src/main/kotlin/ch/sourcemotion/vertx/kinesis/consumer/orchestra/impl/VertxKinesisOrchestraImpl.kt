package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestra
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.OrchestraCodecs
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.registerKinesisOrchestraModules
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticleOptions
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.undeployAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

class VertxKinesisOrchestraImpl(
    private val vertx: Vertx,
    private val options: VertxKinesisOrchestraOptions
) : VertxKinesisOrchestra {

    private var running = false
    private lateinit var deploymentId: String

    override fun start(handler: Handler<AsyncResult<VertxKinesisOrchestra>>) {
        CoroutineScope(vertx.dispatcher()).launch {
            startAwait()
        }.invokeOnCompletion { throwable ->
            throwable?.let { handler.handle(Future.failedFuture(it)) } ?: handler.handle(Future.succeededFuture())
        }
    }

    override suspend fun startAwait() : VertxKinesisOrchestra {
        DatabindCodec.mapper().registerKinesisOrchestraModules()
        DatabindCodec.prettyMapper().registerKinesisOrchestraModules()

        OrchestraCodecs.deployCodecs(vertx.eventBus())

        val awsCredentialsProvider = options.credentialsProviderSupplier.get()
        shareCredentials(awsCredentialsProvider)
        shareFactories(vertx)

        if (options.useCustomShardStatePersistenceService.not()) {
            deployDefaultShardStatePersistence()
        }

        val check = JsonObject.mapFrom(options.asOrchestraVerticleOptions())
        check.mapTo(OrchestrationVerticleOptions::class.java)

        deploymentId =
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
            if (running) {
                vertx.undeploy(deploymentId)
                running = false
            }
        }
        running = true

        return this
    }

    override fun close(handler: Handler<AsyncResult<Unit>>) {
        CoroutineScope(vertx.dispatcher()).launch {
            closeAwait()
        }.invokeOnCompletion { throwable ->
            throwable?.let { handler.handle(Future.failedFuture(it)) } ?: handler.handle(Future.succeededFuture())
        }
    }

    override suspend fun closeAwait() {
        if (running) {
            vertx.undeployAwait(deploymentId)
            running = false
        }
    }

    private suspend fun deployDefaultShardStatePersistence() {
        val options = RedisShardStatePersistenceServiceVerticleOptions(
            options.applicationName,
            options.streamName,
            options.redisOptions,
            options.shardProgressExpiration.toMillis()
        )
        vertx.deployVerticleAwait(
            RedisShardStatePersistenceServiceVerticle::class.java.name, DeploymentOptions().setConfig(
                JsonObject.mapFrom(options)
            )
        )
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
    }
}

