package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumersCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import kotlin.LazyThreadSafetyMode.NONE

/**
 * Verticle that will detect not consumed shards. It's stoppable if the VKCO instance consumes the configured maximum
 * of shards. And it's able to restart detecting as well if consumer(s) was stopped.
 */
class NotConsumedShardDetectorVerticle : CoroutineVerticle() {

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

    private var possibleShardCountToStartConsume = 0
    private var detectionInProgress = false

    override suspend fun start() {
        vertx.eventBus().consumer(
            EventBusAddr.detection.shardsConsumedCountNotification, ::onShardsConsumedCountNotification
        )
        logger.info { "Not consumed shard detector verticle started" }
    }

    override suspend fun stop() {
        stopDetection()
        logger.info { "Not consumed shard detector verticle stopped" }
    }

    private fun onShardsConsumedCountNotification(msg: Message<Int>) {
        val shardsConsumedCount = msg.body()
        if (shardsConsumedCount < options.maxShardCountToConsume) {
            possibleShardCountToStartConsume = options.maxShardCountToConsume - shardsConsumedCount
            startDetection()
        } else {
            stopDetection()
        }
        msg.ack()
    }

    private fun detectNotConsumedShards() {
        if (possibleShardCountToStartConsume <= 0 || detectionInProgress) return
        detectionInProgress = true
        launch {
            val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.clusterName.streamName)
            val finishedShardIds = shardStatePersistence.getFinishedShardIds()
            val unavailableShards =
                shardStatePersistence.getShardIdsInProgress() + finishedShardIds
            val availableShards = streamDescription.shards()
                .filterNot { shard -> unavailableShards.contains(shard.shardIdTyped()) }

            if (availableShards.isNotEmpty()) {
                // On the initial detection we use the configured iterator strategy.
                // Afterwards, on later detected not consumed shards we use existing or latest.
                val shardIdListToConsume =
                    ConsumerShardIdListFactory.create(availableShards, finishedShardIds, possibleShardCountToStartConsume)
                val iteratorStrategy = if (initialDetection) {
                    options.initialIteratorStrategy
                } else {
                    ShardIteratorStrategy.EXISTING_OR_LATEST
                }

                logger.info {
                    "Not consumed shards detected ${availableShards.joinToString { it.shardId() }}. " +
                            "Will send start command for shards ${shardIdListToConsume.joinToString()} according to " +
                            "\"parent(s) must be finished first\" rule."
                }

                val cmd = StartConsumersCmd(shardIdListToConsume, iteratorStrategy)
                vertx.eventBus().requestAwait<Unit>(
                    EventBusAddr.consumerControl.startConsumersCmd, cmd, localOnlyDeliveryOptions
                )
                // Decrease here to avoid unexpected over-commit
                possibleShardCountToStartConsume -= shardIdListToConsume.size
            }
        }.invokeOnCompletion {
            initialDetection = false
            detectionInProgress = false
        }
    }

    private fun startDetection() {
        if (detectionTimerId.isNull()) {
            logger.info { "Start not consumed shards detection" }
            vertx.setPeriodic(options.detectionInterval) {
                detectNotConsumedShards()
            }
        }
    }

    private fun stopDetection() {
        detectionTimerId?.let {
            logger.info { "Stop not consumed shards detection" }
            vertx.cancelTimer(it)
        }
    }

    data class Options(
        /**
         * Maximum this VKCO is configured to consume.
         */
        val clusterName: OrchestraClusterName,
        val maxShardCountToConsume: Int,
        val detectionInterval: Long,
        val initialIteratorStrategy: ShardIteratorStrategy
    )
}
