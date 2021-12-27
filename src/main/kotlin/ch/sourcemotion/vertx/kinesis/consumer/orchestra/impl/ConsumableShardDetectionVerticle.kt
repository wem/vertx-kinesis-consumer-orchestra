package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Redis
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import java.time.Duration

/**
 * Verticle that will detect consumable shards. The detection will stopp if the VKCO instance consumes the configured max.
 * of shards. And it's able to restart detecting as well if consumer(s) was stopped.
 */
internal class ConsumableShardDetectionVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private var initialDetection = true

    private lateinit var options: Options
    private lateinit var redis: Redis
    private lateinit var startCmdDeliveryOptions: DeliveryOptions
    private lateinit var detectionLock: ConsumerDeploymentLock
    private lateinit var kinesisClient: KinesisAsyncClient
    private lateinit var shardStatePersistence : ShardStatePersistenceServiceAsync

    private var detectionTimerId: Long? = null

    /**
     * Count of the currently possible consumers to start. This reflects the difference between the count of already
     * running consumers and the configured max. shards to consume.
     */
    private var possibleShardCountToStartConsume = 0

    /**
     * Flag to prevent from multiple simultaneous consumable detection runs.
     */
    private var detectionInProgress = false

    override suspend fun start() {
        options = config.mapTo(Options::class.java)
        redis = RedisHeimdall.createLight(vertx, options.redisHeimdallOptions)
        startCmdDeliveryOptions =
            deliveryOptionsOf(localOnly = true, sendTimeout = options.startCmdDeliveryTimeoutMillis)
        detectionLock = ConsumerDeploymentLock(
            redis,
            LuaExecutor(redis),
            RedisKeyFactory(options.clusterName),
            Duration.ofMillis(options.detectionLockExpirationMillis),
            Duration.ofMillis(options.detectionLockAcquisitionIntervalMillis)
        )
        kinesisClient =
            SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
                .createKinesisAsyncClient(vertx.orCreateContext)
        shardStatePersistence = ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)

        vertx.eventBus().localConsumer(
            EventBusAddr.detection.consumedShardCountNotification, ::onConsumedShardCountNotification
        ).completion().await()
        logger.debug { "Consumable shard detector verticle started with an detection interval of ${options.detectionInterval}" }
    }

    override suspend fun stop() {
        stopDetection()
        logger.debug { "Consumable shard detector verticle stopped" }
    }

    /**
     * Consumer of notifications about the count of currently consumed shards.
     */
    private fun onConsumedShardCountNotification(msg: Message<Int>) {
        val currentConsumedShardCount = msg.body()
        possibleShardCountToStartConsume = options.maxShardCountToConsume - currentConsumedShardCount
        if (possibleShardCountToStartConsume > 0) {
            runCatching { startDetection() }
                .onFailure { logger.error(it) { "Unable to start consumable shard detection on stream \"${options.clusterName.streamName}\"" } }
        } else {
            runCatching { stopDetection() }
                .onFailure { logger.warn(it) { "Unable to stop consumable shard detection on stream \"${options.clusterName.streamName}\"" } }
        }
        msg.ack()
    }

    private fun detectNotConsumedShards(@Suppress("UNUSED_PARAMETER") handlerId: Long) {
        if (possibleShardCountToStartConsume <= 0 || detectionInProgress) return
        detectionInProgress = true
        launch {
            val shardIdsToStartConsuming = detectionLock.doLocked {
                val existingShards = kinesisClient.listShardsSafe(options.clusterName.streamName)
                val existingShardIds = existingShards.map { it.shardIdTyped() }
                val finishedShardIdsAsync = async { shardStatePersistence.getFinishedShardIds(existingShardIds) }
                val shardIdsInProgressAsync = async { shardStatePersistence.getShardIdsInProgress(existingShardIds) }
                val finishedShardIds = finishedShardIdsAsync.await()
                val shardIdsInProgress = shardIdsInProgressAsync.await()
                val unavailableShardIds = shardIdsInProgress + finishedShardIds

                val availableShards = existingShards
                    .filterNot { shard -> unavailableShardIds.contains(shard.shardIdTyped()) }

                ConsumableShardIdListFactory.create(
                    existingShards,
                    availableShards,
                    finishedShardIds,
                    possibleShardCountToStartConsume
                ).also {
                    if (shardStatePersistence.flagShardsInProgress(it, options.startCmdDeliveryTimeoutMillis).not()) {
                        logger.warn { "Unable to flag all shards ${it.joinToString(",")} as in progress. Cancel detection run." }
                        return@launch
                    }
                }
            }

            if (shardIdsToStartConsuming.isNotEmpty()) {

                // On the initial detection we use the configured iterator strategy.
                // Afterwards, on later detected not consumed shards we use existing or latest,
                // because any later detected consumable shard was either resharded or no more consumed by other VKCO instance.
                val iteratorStrategy = if (initialDetection) {
                    options.initialIteratorStrategy
                } else {
                    ShardIteratorStrategy.EXISTING_OR_LATEST
                }

                logger.info {
                    "Send start command for shards ${shardIdsToStartConsuming.joinToString()} with iterator " +
                            "strategy \"$iteratorStrategy\" according to \"parent(s) must be finished first\" rule."
                }

                val successfulStates = shardIdsToStartConsuming.map { shardIdToStart ->
                    runCatching {
                        vertx.eventBus().request<Unit>(
                            EventBusAddr.consumerControl.startConsumerCmd,
                            StartConsumerCmd(shardIdToStart, iteratorStrategy),
                            startCmdDeliveryOptions
                        ).await()
                        true
                    }.getOrElse {
                        shardStatePersistence.flagShardNoMoreInProgress(shardIdToStart)
                        false
                    }
                }
                if (successfulStates.all { it }) {
                    logger.info { "Start command for consumers of shards ${shardIdsToStartConsuming.joinToString()} successful" }
                }
            } else {
                logger.info { "Consumable shard detection did run, but there was no consumable shard to consume on stream \"${options.clusterName.streamName}\"" }
            }
        }.invokeOnCompletion {
            initialDetection = false
            detectionInProgress = false
        }
    }

    /**
     * Starts the detection. It will run as long as this VKCO instance is NOT consuming the configured
     * max. count of shards.
     *
     * @see [onConsumedShardCountNotification]
     */
    private fun startDetection() {
        if (detectionTimerId.isNull()) {
            logger.info { "Start consumable shards detection on stream \"${options.clusterName.streamName}\"" }
            detectionTimerId = vertx.setPeriodic(options.detectionInterval, ::detectNotConsumedShards)
        } else {
            logger.info { "Consumable shards detection already running" }
        }
    }

    /**
     * Stop of the detection. This will happen if this VKCO instance is currently consuming the configured max.
     * count of shards.
     *
     * @see [onConsumedShardCountNotification]
     */
    private fun stopDetection() {
        detectionTimerId?.let {
            logger.info { "Stop consumable shards detection on stream \"${options.clusterName.streamName}\"" }
            detectionTimerId = null
            vertx.cancelTimer(it)
        }
    }

    data class Options(
        val clusterName: OrchestraClusterName,
        val maxShardCountToConsume: Int,
        val detectionInterval: Long,
        val detectionLockExpirationMillis: Long,
        val detectionLockAcquisitionIntervalMillis: Long,
        val startCmdDeliveryTimeoutMillis: Long = DeliveryOptions.DEFAULT_TIMEOUT,
        val initialIteratorStrategy: ShardIteratorStrategy,
        val redisHeimdallOptions: RedisHeimdallOptions
    )
}
