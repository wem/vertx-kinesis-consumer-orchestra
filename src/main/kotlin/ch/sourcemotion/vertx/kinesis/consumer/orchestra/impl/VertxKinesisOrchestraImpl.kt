package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.KCLV1ImportOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestra
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing.BalancingVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cluster.RedisNodeScoreVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.OrchestraCodecs
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.registerKinesisOrchestraModules
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer.KCLV1Importer
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer.KCLV1ImporterCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.NettyKinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle
import io.vertx.core.*
import io.vertx.core.impl.ContextInternal
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import java.util.*

internal class VertxKinesisOrchestraImpl(
    private val vertx: Vertx,
    private val options: VertxKinesisOrchestraOptions
) : VertxKinesisOrchestra {

    private companion object : KLogging()

    private var running = false
    private val subsystemDeploymentIds = ArrayList<String>()

    override fun start(): Future<VertxKinesisOrchestra> {
        val promise = Promise.promise<VertxKinesisOrchestra>()
        CoroutineScope(vertx.dispatcher()).launch {
            DatabindCodec.mapper().registerKinesisOrchestraModules()
            DatabindCodec.prettyMapper().registerKinesisOrchestraModules()

            OrchestraCodecs.deployCodecs(vertx.eventBus())

            val clusterName = OrchestraClusterName(options.applicationName, options.streamName)
            val clusterNodeId = OrchestraClusterNodeId(clusterName, options.nodeId ?: "${UUID.randomUUID()}")

            val awsCredentialsProvider = options.credentialsProviderSupplier.get()
            shareCredentials(awsCredentialsProvider)
            shareKinesisClientFactories(vertx)

            if (options.useCustomShardStatePersistenceService.not()) {
                deployDefaultShardStatePersistence()
            }
            deployNodeScoreVerticle(clusterNodeId)

            val kclV1ImportOptions = options.kclV1ImportOptions
            if (kclV1ImportOptions.isNotNull()) {
                deployKCL1Importer(kclV1ImportOptions)
            }

            deployReshardingVerticle(clusterName)
            deployConsumableShardDetectorVerticle(clusterName)

            deployConsumerControlVerticle()

            deployBalancingVerticle(clusterNodeId)

            scheduleLastDefenseClose()
            running = true
        }.invokeOnCompletion { throwable ->
            if (throwable != null) {
                val cause = if (throwable is VertxKinesisConsumerOrchestraException) {
                    throwable
                } else VertxKinesisConsumerOrchestraException("Failed to start VKCO", throwable)
                promise.fail(cause)
            } else {
                promise.complete(this)
            }
        }
        return promise.future()
    }

    override fun close(): Future<Unit> {
        val promise = Promise.promise<Unit>()
        CoroutineScope(vertx.dispatcher()).launch {
            subsystemDeploymentIds.reversed().forEach { runCatching { vertx.undeploy(it).await() } }
            promise.complete()
        }
        return promise.future()
    }


    private suspend fun deployConsumerControlVerticle() {
        val consumerControlDeploymentId =
            runCatching {
                vertx.deployVerticle(
                    ConsumerControlVerticle::class.java.name,
                    DeploymentOptions().setConfig(JsonObject.mapFrom(options.asConsumerControlOptions()))
                ).await()
            }.getOrElse {
                throw VertxKinesisConsumerOrchestraException("Unable to start Kinesis consumer orchestra", it)
            }
        subsystemDeploymentIds.add(consumerControlDeploymentId)
    }

    private fun scheduleLastDefenseClose() {
        val context = vertx.orCreateContext
        if (context is ContextInternal) {
            context.addCloseHook { closePromise ->
                if (running) {
                    logger.info { "Close Kinesis consumer orchestra by hook" }
                    close()
                        .onSuccess { closePromise.complete() }
                        .onFailure { closePromise.fail(it) }
                }
            }
        }
    }

    private suspend fun deployConsumableShardDetectorVerticle(clusterName: OrchestraClusterName) {
        val options = ConsumableShardDetectionVerticle.Options(clusterName)
        subsystemDeploymentIds.add(deployVerticle<ConsumableShardDetectionVerticle>(options))
    }

    private suspend fun deployNodeScoreVerticle(clusterNodeId: OrchestraClusterNodeId) {
        val options =
            RedisNodeScoreVerticle.Options(clusterNodeId, options.redisOptions, options.balancing.nodeKeepAliveMillis)
        subsystemDeploymentIds.add(deployVerticle<RedisNodeScoreVerticle>(options))
    }

    private suspend fun deployBalancingVerticle(clusterNodeId: OrchestraClusterNodeId) {
        val balancingOptions = options.balancing
        val options =
            BalancingVerticle.Options(
                clusterNodeId,
                options.redisOptions,
                balancingOptions.activeBalancerCheckIntervalMillis,
                balancingOptions.initialBalancingDelayMillis,
                balancingOptions.balancingIntervalMillis,
                balancingOptions.balancingCommandTimeoutMillis
            )
        subsystemDeploymentIds.add(deployVerticle<BalancingVerticle>(options))
    }

    private suspend fun deployReshardingVerticle(clusterName: OrchestraClusterName) {
        val options = ReshardingVerticle.Options(clusterName, options.redisOptions)
        subsystemDeploymentIds.add(deployVerticle<ReshardingVerticle>(options))
    }

    private suspend fun deployDefaultShardStatePersistence() {
        val options = RedisShardStatePersistenceServiceVerticle.Options(
            options.applicationName,
            options.streamName,
            options.redisOptions,
            options.shardProgressExpiration.toMillis()
        )
        subsystemDeploymentIds.add(deployVerticle<RedisShardStatePersistenceServiceVerticle>(options))
    }

    private suspend inline fun <reified V : Verticle> deployVerticle(options: Any): String {
        return vertx.deployVerticle(
            V::class.java.name,
            deploymentOptionsOf(config = JsonObject.mapFrom(options))
        ).await()
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

        vertx.deployVerticle(
            KCLV1Importer::class.java.name,
            deploymentOptionsOf(config = JsonObject.mapFrom(kclImportOptions))
        ).await()

        logger.info {
            "KCL V1 importer deployed. Will get queried if the shard iterator strategy is " +
                    "ShardIteratorStrategy.EXISTING_OR_LATEST and VKCO has no knowledge about an shard iterator."
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
    private fun shareKinesisClientFactories(vertx: Vertx) {
        SharedData.shareInstance(
            vertx,
            KinesisAsyncClientFactory(
                vertx,
                options.region,
                options.kinesisClientOptions,
                options.awsClientMetricOptions
            ),
            KinesisAsyncClientFactory.SHARED_DATA_REF
        )

        val enhanceFanoutOptions = options.fetcherOptions.enhancedFanOut
        if (enhanceFanoutOptions?.useSdkNettyClient == true) {
            SharedData.shareInstance(
                vertx,
                NettyKinesisAsyncClientFactory(
                    vertx,
                    options.region,
                    options.kinesisClientOptions,
                    options.awsClientMetricOptions,
                    enhanceFanoutOptions.sdkNettyMaxConcurrency,
                    enhanceFanoutOptions.sdkNettyMaxStreams
                ),
                NettyKinesisAsyncClientFactory.SHARED_DATA_REF
            )
        }
    }
}

