package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle.Companion.CONSUMER_START_CMD_ADDR
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReOrchestrationCmdDispatcher
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardProcessingBundle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.DeploymentOptions
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.undeployAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.RedisOptions
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Duration

class OrchestrationVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private var consumersDeploymentId: String? = null

    private val options by lazy {
        config.mapTo(OrchestrationVerticleOptions::class.java)
    }

    private val redis by lazy { Redis.createClient(vertx, options.redisOptions) }

    private val kinesisClient by lazy {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private val shardStatePersistence by lazy {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(
            vertx
        )
    }

    private val consumerDeploymentLock by lazy {
        val redisApi = RedisAPI.api(redis)
        ConsumerDeploymentLock(
            redisApi,
            LuaExecutor(redisApi),
            RedisKeyFactory(options.applicationName, options.streamName),
            Duration.ofMillis(options.consumerDeploymentLockExpiration),
            Duration.ofMillis(options.consumerDeploymentLockRetryInterval)
        )
    }

    private val reOrchestrationCmdDispatcher: ReOrchestrationCmdDispatcher by lazy {
        ReOrchestrationCmdDispatcher.create(
            vertx,
            options.applicationName,
            options.streamName,
            kinesisClient,
            shardStatePersistence,
            this,
            options.redisOptions,
            reshardingEventHandler = this::reOrchestrate
        )
    }

    override suspend fun start() {
        logger.debug { "Starting orchestrating with options \"$options\"" }
        reOrchestrationCmdDispatcher.start()
        consumerDeploymentLock.doLocked { deployConsumerVerticles() }
    }

    override suspend fun stop() {
        runCatching { reOrchestrationCmdDispatcher.stop() }
        runCatching { redis.close() }
    }

    private suspend fun deployConsumerVerticles(forceExistingShardIterator: Boolean = false) {
        logger.debug { "Start to deploy consumer verticle" }
        val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.streamName)

        val shardIdsInProgress = shardStatePersistence.getShardIdsInProgress()
        val finishedShardIds = shardStatePersistence.getFinishedShardIds()
        // Available shards are the existing one, they are not already in progress or even finished
        val availableShards = streamDescription.shards()
            .filterNot { shard -> shardIdsInProgress.contains(shard.shardIdTyped()) }
            .filterNot { shard -> finishedShardIds.contains(shard.shardIdTyped()) }

        val processingBundle = ShardProcessingBundle.create(
            availableShards,
            finishedShardIds,
            options.loadConfiguration
        )

        logger.info { "This orchestra will process the shards \"${processingBundle.shardIds.joinToString(", ")}\" on stream \"${streamDescription.streamName()}\"" }

        val consumerConfig = if (forceExistingShardIterator) {
            createKinesisConsumerVerticleConfig(ShardIteratorStrategy.EXISTING_OR_LATEST)
        } else {
            createKinesisConsumerVerticleConfig()
        }

        consumersDeploymentId = vertx.deployVerticleAwait(
            options.consumerVerticleClass,
            DeploymentOptions().setInstances(processingBundle.shardIds.size)
                // We merge the library internal consumer verticle configuration with the user provided one.
                .setConfig(JsonObject.mapFrom(consumerConfig).mergeIn(JsonObject(options.consumerVerticleConfig)))
        )

        processingBundle.shardIds.forEach { shardId ->
            vertx.eventBus()
                .requestAwait<Unit>(CONSUMER_START_CMD_ADDR, "$shardId", DeliveryOptions().setLocalOnly(true))
        }

        logger.debug { "\"${processingBundle.shardIds.size}\" Kinesis consumer verticles deployed and started" }
    }

    /**
     * Called when a resharding is done. Means on split immediately or on merge when both parent shards are finished.
     */
    private fun reOrchestrate() {
        launch {
            consumerDeploymentLock.doLocked {
                redeployConsumerVerticles()
            }
        }
    }

    private suspend fun redeployConsumerVerticles() {
        // First undeploy consumers
        consumersDeploymentId?.let { vertx.undeployAwait(it) }
        consumersDeploymentId = null

        deployConsumerVerticles(true)
        // Notify external interested consumer
        vertx.eventBus().send(options.reshardingNotificationAddress, null)
    }

    private fun createKinesisConsumerVerticleConfig(shardIteratorStrategyOverride: ShardIteratorStrategy? = null) =
        KinesisConsumerVerticleOptions(
            options.applicationName,
            options.streamName,
            shardIteratorStrategyOverride ?: options.shardIteratorStrategy,
            options.errorHandling,
            options.kinesisPollInterval,
            options.recordsPerPollLimit,
            options.redisOptions
        )
}

@JsonIgnoreProperties(ignoreUnknown = true)
internal class OrchestrationVerticleOptions(
    var applicationName: String,
    var streamName: String,
    // We not force the user to add Java date Jackson module
    var kinesisPollInterval: Long,
    var recordsPerPollLimit: Int,
    var redisOptions: RedisOptions,

    var shardIteratorStrategy: ShardIteratorStrategy,
    var loadConfiguration: LoadConfiguration,
    var errorHandling: ErrorHandling,
    // We not force the user to add Java date Jackson module
    val consumerDeploymentLockExpiration: Long,
    // We not force the user to add Java date Jackson module
    val consumerDeploymentLockRetryInterval: Long,
    var reshardingNotificationAddress: String,
    var consumerVerticleClass: String,
    var consumerVerticleConfig: Map<String, Any>
)
