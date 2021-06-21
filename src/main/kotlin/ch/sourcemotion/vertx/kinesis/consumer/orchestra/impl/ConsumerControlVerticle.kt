package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.*
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
import io.vertx.redis.client.Redis
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Duration
import kotlin.LazyThreadSafetyMode.NONE

internal class ConsumerControlVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private val consumerDeploymentIds = HashMap<ShardId, String>()

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    private val redis: Redis by lazy(NONE) { RedisHeimdall.createLight(vertx, options.redisHeimdallOptions) }

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

        notifyAboutActiveConsumers() // Directly after start there would be no consumer, so we start the detection.
        logger.debug { "Consumer control started with options \"$options\"" }
        logger.info { "VKCO instance configure to consume max. ${options.loadConfiguration.maxShardsCount} " +
                "shards from stream ${options.clusterName.streamName}" }
    }

    private fun onStopConsumerCmd(msg: Message<StopConsumerCmd>) {
        val shardId = msg.body().shardId
        val deploymentIdToUndeploy = consumerDeploymentIds.remove(shardId)
        if (deploymentIdToUndeploy.isNotNullOrBlank()) {
            vertx.undeploy(deploymentIdToUndeploy) {
                if (it.succeeded()) {
                    logger.info { "Consumer for shard \"$shardId\" stopped" }
                    msg.ack()
                } else {
                    msg.fail(StopConsumerCmd.CONSUMER_STOP_FAILURE, it.cause().message)
                }
            }
        } else {
            logger.info { "Unable to stop consumer for shard \"$shardId\", because no known consumer for this shard" }
            msg.fail(StopConsumerCmd.UNKNOWN_CONSUMER_FAILURE, "Unknown consumer")
        }
        logAndNotifyAboutActiveConsumers()
    }

    private fun onStartConsumersCmd(msg: Message<StartConsumersCmd>) {
        val cmd = msg.body()
        if (cmd.shardIds.isEmpty()) {
            msg.ack()
            return
        }
        // If this VKCO instance is already consuming the configured max. count of shards we skip and reply failure
        if (consumerCapacity() <= 0) {
            logger.warn { "Consumer control unable to start consumer(s) for shards ${cmd.shardIds.joinToString()} because no consumer capacity left" }
            logAndNotifyAboutActiveConsumers()
            msg.fail(StartConsumersCmd.CONSUMER_CAPACITY_FAILURE, "No consumer capacity left")
            return
        }
        launch {
            consumerDeploymentLock.doLocked {

                // We filter the list of shard ids to consume for they got in progress in the meanwhile.
                // This can happen if 2 or more VKCO instances did trigger a consumer start at the (near) exactly same time.
                val shardIdsToConsume = cmd.shardIds.filterUnavailableShardIds().take(consumerCapacity())

                // If just a sub set of shards cannot be consumed because of the left capacity, we log the left ones.
                cmd.shardIds.filterNot { shardIdsToConsume.contains(it) }.also {
                    if (it.isNotEmpty()) {
                        logger.info {
                            "Consumer control unable to start consumer(s) for shards ${it.joinToString()} " +
                                    "because of its consumer limit of \"${options.loadConfiguration.maxShardsCount}\""
                        }
                    }
                }

                shardIdsToConsume.forEach { shardId ->
                    val consumerOptions = createConsumerVerticleOptions(shardId, cmd.iteratorStrategy)
                    startConsumer(
                        options.consumerVerticleClass,
                        JsonObject(options.consumerVerticleConfig),
                        consumerOptions
                    )
                }
            }
        }.invokeOnCompletion {
            logAndNotifyAboutActiveConsumers()
            if (it == null) {
                msg.ack()
            } else {
                logger.warn(it) { "Consumer control unable to start consumers for shards \"${cmd.shardIds.joinToString()}\"" }
                msg.fail(StartConsumersCmd.CONSUMER_START_FAILURE, it.message)
            }
        }
    }

    private suspend fun startConsumer(
        consumerVerticleClass: String,
        consumerVerticleConfig: JsonObject,
        consumerOptions: KinesisConsumerVerticleOptions
    ) {
        vertx.runCatching {
            deployVerticleAwait(
                consumerVerticleClass,
                deploymentOptionsOf(config = JsonObject.mapFrom(consumerOptions).mergeIn(consumerVerticleConfig, true))
            )
        }.onSuccess { deploymentId ->
            consumerDeploymentIds[consumerOptions.shardId] = deploymentId
            logger.info { "Consumer started for stream ${options.clusterName.streamName} / shard \"${consumerOptions.shardId}\"." }
        }.onFailure {
            logger.error(it) { "Failed to start consumer of stream ${options.clusterName.streamName} / shard ${consumerOptions.shardId}" }
        }
    }

    private fun logAndNotifyAboutActiveConsumers() {
        if (consumerDeploymentIds.keys.isEmpty()) {
            logger.info { "Currently no shards get consumed on stream \"${options.clusterName.streamName}\". " +
                    "This VKCO instance has to capacity to consume ${consumerCapacity()} shards" }
        } else {
            logger.info { "Currently the shards ${consumerDeploymentIds.keys.joinToString()} " +
                    "are consumed on stream \"${options.clusterName.streamName}\". " +
                    "This VKCO instance has to capacity to consume ${consumerCapacity()} further shards" }
        }
        notifyAboutActiveConsumers()
    }

    private fun notifyAboutActiveConsumers() {
        vertx.eventBus().send(EventBusAddr.detection.consumedShardCountNotification, consumerDeploymentIds.size)
    }

    private suspend fun ShardIdList.filterUnavailableShardIds(): ShardIdList {
        val existingShardIds = kinesisClient.listShardsSafe(options.clusterName.streamName).map { it.shardIdTyped() }
        val unavailableShardIds =
            shardStatePersistence.getFinishedShardIds(existingShardIds) + shardStatePersistence.getShardIdsInProgress(
                existingShardIds
            )
        return filterNot { unavailableShardIds.contains(it) }
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
            options.sequenceNumberImportAddress,
            options.shardProgressExpirationMillis,
            options.fetcherOptions
        )

    @JsonIgnoreProperties(ignoreUnknown = true)
    internal class Options(
        val clusterName: OrchestraClusterName,
        val redisHeimdallOptions: RedisHeimdallOptions,
        val fetcherOptions: FetcherOptions,

        val shardIteratorStrategy: ShardIteratorStrategy,
        val loadConfiguration: LoadConfiguration,
        val errorHandling: ErrorHandling,
        val consumerDeploymentLockExpiration: Long,
        val consumerDeploymentLockRetryInterval: Long,
        val consumerVerticleClass: String,
        val consumerVerticleConfig: Map<String, Any>,
        val sequenceNumberImportAddress: String? = null,
        val shardProgressExpirationMillis: Long
    )
}


internal fun VertxKinesisOrchestraOptions.asConsumerControlOptions() = ConsumerControlVerticle.Options(
    OrchestraClusterName(applicationName, streamName),
    redisOptions,
    fetcherOptions,
    shardIteratorStrategy,
    loadConfiguration,
    errorHandling,
    consumerDeploymentLockExpiration.toMillis(),
    consumerDeploymentLockRetryInterval.toMillis(),
    consumerVerticleClass,
    consumerVerticleOptions.map,
    kclV1ImportOptions?.importAddress,
    shardProgressExpiration.toMillis()
)
