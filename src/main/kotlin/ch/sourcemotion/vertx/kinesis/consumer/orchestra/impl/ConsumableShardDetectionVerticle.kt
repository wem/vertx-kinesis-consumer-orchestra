package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumersCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import kotlin.LazyThreadSafetyMode.NONE

/**
 * Verticle that will detect consumable shards. The detection will stopp if the VKCO instance consumes the configured max.
 * of shards. And it's able to restart detecting as well if consumer(s) was stopped.
 */
internal class ConsumableShardDetectionVerticle : CoroutineVerticle() {

    private companion object : KLogging() {
        private val localOnlyDeliveryOptions = deliveryOptionsOf(localOnly = true)
    }

    private var initialDetection = true

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    private val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private val shardStatePersistence by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

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
        vertx.eventBus().localConsumer(
            EventBusAddr.detection.consumedShardCountNotification, ::onConsumedShardCountNotification
        ).completion().await()
        logger.debug { "Consumable shard detector verticle started" }
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

    private fun detectNotConsumedShards(handlerId: Long) {
        if (possibleShardCountToStartConsume <= 0 || detectionInProgress) return
        detectionInProgress = true
        launch {
            val existingShards = kinesisClient.listShards { it.streamName(options.clusterName.streamName) }.await().shards()
            val finishedShardIds = shardStatePersistence.getFinishedShardIds()
            val shardIdsInProgress = shardStatePersistence.getShardIdsInProgress()
            val unavailableShardIds = shardIdsInProgress + finishedShardIds

            val availableShards = existingShards
                .filterNot { shard -> unavailableShardIds.contains(shard.shardIdTyped()) }

            val shardIdListToConsume =
                ConsumableShardIdListFactory.create(existingShards, availableShards, finishedShardIds, possibleShardCountToStartConsume)

            if (shardIdListToConsume.isNotEmpty()) {

                // On the initial detection we use the configured iterator strategy.
                // Afterwards, on later detected not consumed shards we use existing or latest,
                // because any later detected consumable shard was either resharded or no more consumed by other VKCO instance.
                val iteratorStrategy = if (initialDetection) {
                    options.initialIteratorStrategy
                } else {
                    ShardIteratorStrategy.EXISTING_OR_LATEST
                }

                logger.info {
                    "Consumable shards detected ${availableShards.joinToString { it.shardId() }}. " +
                            "Will send start command for shards ${shardIdListToConsume.joinToString()} with iterator " +
                            "strategy \"$iteratorStrategy\" according to \"parent(s) must be finished first\" rule."
                }

                val cmd = StartConsumersCmd(shardIdListToConsume, iteratorStrategy)
                vertx.eventBus().request<Unit>(
                    EventBusAddr.consumerControl.startConsumersCmd, cmd, localOnlyDeliveryOptions
                ).await()
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
        val initialIteratorStrategy: ShardIteratorStrategy
    )
}
