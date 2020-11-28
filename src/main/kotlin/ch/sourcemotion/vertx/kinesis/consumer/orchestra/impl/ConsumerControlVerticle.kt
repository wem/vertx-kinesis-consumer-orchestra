package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumersCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StopConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Duration
import kotlin.LazyThreadSafetyMode.NONE

internal class ConsumerControlVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private val consumerDeploymentIds = HashMap<ShardId, String>()

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    private val redis by lazy(NONE) { RedisHeimdall.create(vertx, options.redisHeimdallOptions) }

    private val shardStatePersistence by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(
            vertx
        )
    }

    private val consumerDeploymentLock by lazy(NONE) {
        ConsumerDeploymentLock(
            redis,
            LuaExecutor(redis),
            RedisKeyFactory(options.clusterName),
            Duration.ofMillis(options.consumerDeploymentLockExpiration),
            Duration.ofMillis(options.consumerDeploymentLockRetryInterval)
        )
    }

    override suspend fun start() {
        vertx.eventBus().localConsumer(EventBusAddr.consumerControl.stopConsumerCmd, ::onStopConsumerCmd)
            .completionHandlerAwait()
        vertx.eventBus().localConsumer(EventBusAddr.consumerControl.startConsumersCmd, ::onStartConsumersCmd)
            .completionHandlerAwait()

        notifyAboutActiveConsumers() // Directly after start there would be no consumer, but we start the detection
        logger.debug { "Consumer control started with options \"$options\"" }
    }

    private fun onStopConsumerCmd(msg: Message<StopConsumerCmd>) {
        val shardId = msg.body().shardId
        val deploymentIdToUndeploy = consumerDeploymentIds.remove(shardId)
        if (deploymentIdToUndeploy.isNotNullOrBlank()) {
            vertx.undeploy(deploymentIdToUndeploy) {
                if (it.succeeded()) {
                    msg.ack()
                } else {
                    msg.fail(0, it.cause().message)
                }
            }
        } else {
            msg.fail(0, "No consumer deployed for shard $shardId, so cannot be stopped")
        }
        logAndNotifyAboutActiveConsumers()
    }

    private fun onStartConsumersCmd(msg: Message<StartConsumersCmd>) {
        val cmd = msg.body()
        if (cmd.shardIds.isEmpty()) {
            msg.ack()
            return
        }
        launch {
            consumerDeploymentLock.doLocked {
                // We verify that the command not contains more shards to start as we can (configured to).
                val shardIdsToConsume = cmd.shardIds.take(consumerCapacity())
                if (shardIdsToConsume.isEmpty()) {
                    msg.fail(
                        0,
                        "Consumer control has reached its configured limit of \"${options.loadConfiguration.maxShardsCount}\" consumers. " +
                                "So shards ${cmd.shardIds.joinToString()} will not consumed by this VKCO instance."
                    )
                    return@doLocked
                }

                // If a sub set of shards cannot be consumed, we just log the left ones.
                cmd.shardIds.filterNot { shardIdsToConsume.contains(it) }.also {
                    if (it.isNotEmpty()) {
                        logger.info {
                            "Consumer control is not able to consume shards ${it.joinToString()} " +
                                    "because of its limit of \"${options.loadConfiguration.maxShardsCount}\" consumers"
                        }
                    }
                }

                shardIdsToConsume.forEach { shardId ->
                    if (isShardAvailable(shardId)) {
                        // On start consumer is called for resharding. In this situation the sequence number to start is present.
                        val consumerOptions = createConsumerVerticleOptions(shardId, cmd.iteratorStrategy)
                        startConsumer(options.consumerVerticleClass, JsonObject(options.consumerVerticleConfig), consumerOptions)
                    } else {
                        logger.debug { "Shard $shardId already finished or consumer in progress." }
                    }
                }
                logAndNotifyAboutActiveConsumers()
                msg.ack()
            }
        }
    }

    private suspend fun startConsumer(consumerVerticleClass: String, consumerVerticleConfig: JsonObject, consumerOptions: KinesisConsumerVerticleOptions) {
        vertx.runCatching {
            deployVerticleAwait(
                consumerVerticleClass,
                deploymentOptionsOf(config = JsonObject.mapFrom(consumerOptions).mergeIn(consumerVerticleConfig, true))
            )
        }.onSuccess { deploymentId ->
            consumerDeploymentIds[consumerOptions.shardId] = deploymentId
            logger.info { "Consumer started for shard ${consumerOptions.shardId}" }
        }.onFailure {
            logger.error(it) { "Failed to start consumer of shard ${consumerOptions.shardId}" }
        }
    }

    private fun logAndNotifyAboutActiveConsumers() {
        if (consumerDeploymentIds.keys.isEmpty()) {
            logger.info { "Currently no shards get consumed." }
        } else {
            logger.info { "Currently the shards ${consumerDeploymentIds.keys.joinToString()} are consumed." }
        }
        notifyAboutActiveConsumers()
    }

    private fun notifyAboutActiveConsumers() {
        vertx.eventBus().send(EventBusAddr.detection.activeConsumerCountNotification, consumerDeploymentIds.size)
    }

    private suspend fun isShardAvailable(shardId: ShardId): Boolean {
        val unavailableShardIds =
            shardStatePersistence.getFinishedShardIds() + shardStatePersistence.getShardIdsInProgress()
        return unavailableShardIds.contains(shardId).not()
    }

    private fun consumerCapacity() = options.loadConfiguration.maxShardsCount - consumerDeploymentIds.size

    private fun createConsumerVerticleOptions(
        shardId: ShardId,
        shardIteratorStrategy: ShardIteratorStrategy
    ) =
        KinesisConsumerVerticleOptions(
            shardId,
            options.clusterName,
            shardIteratorStrategy,
            options.errorHandling,
            options.kinesisFetchInterval,
            options.recordsPerBatchLimit,
            options.sequenceNumberImportAddress
        )

    @JsonIgnoreProperties(ignoreUnknown = true)
    internal class Options(
        val clusterName: OrchestraClusterName,
        // We not force the user to add Java date Jackson module
        var kinesisFetchInterval: Long,
        var recordsPerBatchLimit: Int,
        var redisHeimdallOptions: RedisHeimdallOptions,

        var shardIteratorStrategy: ShardIteratorStrategy,
        var loadConfiguration: LoadConfiguration,
        var errorHandling: ErrorHandling,
        // We not force the user to add Java date Jackson module
        val consumerDeploymentLockExpiration: Long,
        // We not force the user to add Java date Jackson module
        val consumerDeploymentLockRetryInterval: Long,
        var consumerVerticleClass: String,
        var consumerVerticleConfig: Map<String, Any>,
        val sequenceNumberImportAddress: String? = null
    )
}


internal fun VertxKinesisOrchestraOptions.asConsumerControlOptions() = ConsumerControlVerticle.Options(
    OrchestraClusterName(applicationName, streamName),
    kinesisFetchInterval.toMillis(),
    recordsPerBatchLimit,
    redisOptions,
    shardIteratorStrategy,
    loadConfiguration,
    errorHandling,
    consumerDeploymentLockExpiration.toMillis(),
    consumerDeploymentLockRetryInterval.toMillis(),
    consumerVerticleClass,
    consumerVerticleOptions.map,
    kclV1ImportOptions?.importAddress
)
