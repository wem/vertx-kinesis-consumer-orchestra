package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.KCLV1ImportOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestra
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.OrchestraCodecs
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.registerKinesisOrchestraModules
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer.KCLV1Importer
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer.KCLV1ImporterCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticleOptions
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.core.undeployAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

class VertxKinesisOrchestraImpl(
    private val vertx: Vertx,
    private val options: VertxKinesisOrchestraOptions
) : VertxKinesisOrchestra {

    private companion object : KLogging()

    private var running = false
    private lateinit var orchestrationVerticleDeploymentId: String
    private lateinit var shardPersistenceDeploymentId: String

    override fun start(handler: Handler<AsyncResult<VertxKinesisOrchestra>>) {
        CoroutineScope(vertx.dispatcher()).launch {
            startAwait()
        }.invokeOnCompletion { throwable ->
            throwable?.let { handler.handle(Future.failedFuture(it)) } ?: handler.handle(Future.succeededFuture())
        }
    }

    override suspend fun startAwait(): VertxKinesisOrchestra {
        DatabindCodec.mapper().registerKinesisOrchestraModules()
        DatabindCodec.prettyMapper().registerKinesisOrchestraModules()

        OrchestraCodecs.deployCodecs(vertx.eventBus())

        val awsCredentialsProvider = options.credentialsProviderSupplier.get()
        shareCredentials(awsCredentialsProvider)
        shareFactories(vertx)

        if (options.useCustomShardStatePersistenceService.not()) {
            deployDefaultShardStatePersistence()
        }

        val kclV1ImportOptions = options.kclV1ImportOptions
        if(kclV1ImportOptions.isNotNull()) {
            deployKCL1Importer(kclV1ImportOptions)
        }

        val check = JsonObject.mapFrom(options.asOrchestraVerticleOptions())
        check.mapTo(OrchestrationVerticleOptions::class.java)

        orchestrationVerticleDeploymentId =
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
        scheduleLastDefenseClose()
        running = true

        return this
    }

    private fun scheduleLastDefenseClose() {
        val context = vertx.orCreateContext
        context.addCloseHook {
            if (running) {
                logger.info { "Close Kinesis consumer orchestra by hook" }
                CoroutineScope(context.dispatcher()).launch {
                    closeAwait()
                }
            }
        }
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
            vertx.undeployAwait(orchestrationVerticleDeploymentId)
            vertx.undeployAwait(shardPersistenceDeploymentId)
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
        shardPersistenceDeploymentId = vertx.deployVerticleAwait(
            RedisShardStatePersistenceServiceVerticle::class.java.name, DeploymentOptions().setConfig(
                JsonObject.mapFrom(options)
            )
        )
    }

    private suspend fun deployKCL1Importer(kclImportOptions: KCLV1ImportOptions) {
        val credentialsProviderSupplier = kclImportOptions.credentialsProviderSupplier
        if (credentialsProviderSupplier.isNotNull()) {
            val kclV1CredentialsProvider = KCLV1ImporterCredentialsProvider(credentialsProviderSupplier.get())
            SharedData.shareInstance(
                vertx,
                kclV1CredentialsProvider,
                KCLV1ImporterCredentialsProvider.SHARED_DATA_REF
            )
        }

        vertx.deployVerticleAwait(
            KCLV1Importer::class.java.name,
            deploymentOptionsOf(config = JsonObject.mapFrom(kclImportOptions))
        )

        logger.info { "KCL V1 importer deployed. Will get queried if the shard iterator strategy is " +
                "ShardIteratorStrategy.EXISTING_OR_LATEST and VKCO has no knowledge about an shard iterator." }
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

