package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumersCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StopConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange
import java.time.Duration
import kotlin.LazyThreadSafetyMode.NONE

internal class ReshardingVerticle : CoroutineVerticle() {

    private companion object : KLogging() {
        private val localOnlyDeliveryOptions = deliveryOptionsOf(localOnly = true)
    }

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    private val shardStatePersistence by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    private val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    override suspend fun start() {
        vertx.eventBus().localConsumer(EventBusAddr.resharding.notification, this::onReshardingEvent)
    }

    /**
     * Called if a shard, consumed by this VKCO instance got resharded.
     */
    private fun onReshardingEvent(msg: Message<ReshardingEvent>) {
        msg.ack() // We directly ack as the caller must not be aware of subsequent actions.
        launch {
            when (val event = msg.body()) {
                is MergeReshardingEvent -> onMergeReshardingEvent(event)
                is SplitReshardingEvent -> onSplitReshardingEvent(event)
                else -> logger.error { "Unknown type of resharding event ${event::class.qualifiedName}" }
            }
        }
    }

    private suspend fun onMergeReshardingEvent(event: MergeReshardingEvent) {
        val (parentShardId, childShardId) = event
        logger.info { "Received merge resharding event. Parent shard: $parentShardId, child shard: $childShardId" }

        finishShardConsuming(event, parentShardId)

        val readyToConsumeChildShard = isReadyToConsumeChildShard(event)
        if (readyToConsumeChildShard) {
            sendLocalStartConsumerCmd(childShardId)
        }
    }


    private suspend fun onSplitReshardingEvent(event: SplitReshardingEvent) {
        val (parentShardId, childShardIds) = event
        logger.info { "Received split resharding event. Parent shard: $parentShardId, child shards: $childShardIds" }

        finishShardConsuming(event, parentShardId)

        // We start the first child local
        val firstChildShardId = childShardIds.first()
        sendLocalStartConsumerCmd(firstChildShardId)
    }

    private suspend fun finishShardConsuming(
        event: ReshardingEvent,
        parentShardId: ShardId
    ) {
        persistChildShardsIterators(event)
        saveFinishedShard(parentShardId)
        sendLocalStopShardConsumerCmd(parentShardId)
    }

    /**
     * A shard get flagged as finished here, as elsewhere it could be too early or to late.
     */
    private suspend fun saveFinishedShard(shardId: ShardId) {
        val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.clusterName.streamName)
        // The expiration of the shard finished flag, will be an hour after the shard retention.
        // So it's ensured that we not lose the finished flag of this shard and avoid death data.
        val finishedFlagExpiration = Duration.ofHours(streamDescription.retentionPeriodHours().toLong() + 1).toMillis()
        shardStatePersistence.saveFinishedShard(shardId, finishedFlagExpiration)
        shardStatePersistence.deleteShardSequenceNumber(shardId)
    }

    private suspend fun sendLocalStartConsumerCmd(shardId: ShardId) {
        val cmd = StartConsumersCmd(listOf(shardId), ShardIteratorStrategy.EXISTING_OR_LATEST)
        vertx.eventBus()
            .runCatching { requestAwait<Unit>(EventBusAddr.consumerControl.startConsumersCmd, cmd, localOnlyDeliveryOptions) }
            .onSuccess { logger.info { "Shard $shardId will now be consumed after resharding." } }
            .onFailure { logger.warn(it) { "Shard $shardId will NOT be consumed after resharding. Please restart this VKCO instance." } }
    }

    private suspend fun sendLocalStopShardConsumerCmd(shardId: ShardId) {
        vertx.eventBus()
            .runCatching { requestAwait<Unit>(EventBusAddr.consumerControl.stopConsumerCmd, StopConsumerCmd(shardId), localOnlyDeliveryOptions) }
            .onSuccess { logger.info { "Stop consumer command of shard $shardId success" } }
            .onFailure {
                logger.warn(it) {
                    "Stop consumer of shard $shardId failed. " +
                            "This is basically not critical, but this VKCO instance should be redeployed in long run."
                }
            }
    }


    /**
     * Persists a flag for the parent of the given merge parent shard. So we are able to determine if both parents
     * got finished and therefore the VKCO can continue and conume from the resulting child shard.
     *
     * @return True if both merge parents are finished and both event did reach this organizer Verticle.
     */
    private suspend fun isReadyToConsumeChildShard(mergeReshardingEvent: MergeReshardingEvent): Boolean {
        return (shardStatePersistence.flagMergeParentReadyToReshard(
            mergeReshardingEvent.finishedParentShardId,
            mergeReshardingEvent.childShardId
        )).also { canReOrchestrate ->
            if (canReOrchestrate) {
                val garbageWarnMsg =
                    "There maybe some data garbage on Redis. Looks like not all merge parent ready flags are removed."
                shardStatePersistence.runCatching {
                    deleteMergeParentsReshardingReadyFlag(mergeReshardingEvent.childShardId)
                }.onSuccess {
                    if (it != 2) {
                        logger.warn { garbageWarnMsg }
                    }
                }.onFailure {
                    logger.warn(it) { garbageWarnMsg }
                }
            }
        }
    }

    /**
     * Persist the starting sequence numbers of resharding child shards.
     *
     * This happens here as this should be early as possible and the follow up workflow steps would be much more easier.
     */
    private suspend fun persistChildShardsIterators(reshardingEvent: ReshardingEvent) {
        val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.clusterName.streamName)

        val childShardIds = when (reshardingEvent) {
            is MergeReshardingEvent -> {
                listOf(reshardingEvent.childShardId)
            }
            is SplitReshardingEvent -> {
                reshardingEvent.childShardIds
            }
            else -> throw VertxKinesisConsumerOrchestraException(
                "Resharding of type ${reshardingEvent.reshardingType} is unknown"
            )
        }

        childShardIds.forEach { childShardId ->
            val startingSequenceNumber =
                streamDescription.shards().first { it.shardIdTyped() == childShardId }.sequenceNumberRange()
                    .startSequenceNumberTyped()
            shardStatePersistence.saveConsumerShardSequenceNumber(childShardId, startingSequenceNumber)
        }
    }

    private fun SequenceNumberRange.startSequenceNumberTyped() =
        SequenceNumber(startingSequenceNumber(), SequenceNumberIteratorPosition.AT)


    data class Options(
        val clusterName: OrchestraClusterName,
        val redisHeimdallOptions: RedisHeimdallOptions
    )
}
