package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isFalse
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication.OrchestraCommunication
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
import kotlin.LazyThreadSafetyMode.NONE

internal class ReshardingOrganizerVerticle : CoroutineVerticle() {

    companion object : KLogging() {
        private val localOnlyDeliveryOptions = deliveryOptionsOf(localOnly = true)
    }

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    private val eventBusAddr by lazy(NONE) { EventBusAddr(options.clusterName).resharding }

    private val shardStatePersistence by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    private val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private lateinit var communication: OrchestraCommunication

    override suspend fun start() {
        communication = OrchestraCommunication.create(
            vertx,
            options.redisHeimdallOptions,
            options.clusterName,
            options.useVertxCommunication,
            this::onClusterStartConsumerCmd
        )
        vertx.eventBus().localConsumer(eventBusAddr.reshardingNotification, this::onReshardingEvent)
        vertx.eventBus().localConsumer(eventBusAddr.readyForStartConsumerCmd, this::onReadyForStartConsumerCmd)
    }

    /**
     * Called when this VCKO instance is ready to proceed a start consumer cmd from a remote VKCO instance.
     */
    private fun onReadyForStartConsumerCmd(msg: Message<Unit>) {
        launch {
            communication.readyForShardConsumeCommands()
            msg.ack()
        }
    }

    /**
     * Get called if a VKCO instance requests this instance to start consuming a shard (can also called by self).
     * Called only in the case of a split resharding and when this instance accepts consume shard requests.
     */
    private fun onClusterStartConsumerCmd(shardId: ShardId) {
        launch {
            val shardConsumerStarted =
                vertx.eventBus()
                    .runCatching {
                        requestAwait<Boolean?>(
                            eventBusAddr.startConsumerCmd,
                            shardId,
                            localOnlyDeliveryOptions
                        ).body()
                    }
                    .getOrElse {
                        logger.warn(it) { "Unable to start consumer for remote split resharding request. You should restart this VKCO instance." }
                        false
                    }

            if (shardConsumerStarted.isFalse()) {
                logger.warn { "Unable to start consumer for shard consuming request. You should restart this VKCO instance or even the whole cluster." }
            }
        }
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
        persistChildShardsIterators(event)
        // We stop the consumer of the finished parent shard. So this orchestra instance is able to consume
        // from child shard when the other parent got finished too.
        sendLocalStopShardConsumerCmd(parentShardId)

        val readyToConsumeChildShard = isReadyToConsumeChildShard(event)
        if (readyToConsumeChildShard) {
            sendLocalStartConsumerCmd(childShardId)
        }
    }

    private suspend fun onSplitReshardingEvent(event: SplitReshardingEvent) {
        val (parentShardId, childShardIds) = event
        logger.info { "Received split resharding event. Parent shard: $parentShardId, child shards: $childShardIds" }
        persistChildShardsIterators(event)

        sendLocalStopShardConsumerCmd(parentShardId)

        val firstChildShardId = childShardIds.first()
        val secondChildShardId = childShardIds.last()

        // We start the first child local.
        sendLocalStartConsumerCmd(firstChildShardId)

        // We try to start consumer for the second split child over the VKCO cluster, so also a remote VKCO instance
        // can take over it
        sendClusterStartConsumerCmd(secondChildShardId)
    }

    private suspend fun sendLocalStartConsumerCmd(shardId: ShardId) {
        vertx.eventBus()
            .runCatching { requestAwait<Unit>(eventBusAddr.startConsumerCmd, shardId, localOnlyDeliveryOptions) }
            .onSuccess { logger.info { "Shard $shardId will now be consumed after resharding." } }
            .onFailure { logger.warn(it) { "Shard $shardId will NOT be consumed after resharding. Please restart this VKCO instance." } }
    }

    private suspend fun sendLocalStopShardConsumerCmd(shardId: ShardId) {
        vertx.eventBus()
            .runCatching { requestAwait<Unit>(eventBusAddr.stopConsumerCmd, shardId, localOnlyDeliveryOptions) }
            .onSuccess { logger.info { "Stop consumer command of shard $shardId success" } }
            .onFailure {
                logger.warn(it) {
                    "Stop consumer of shard $shardId failed. " +
                            "This is basically not critical, but this VKCO instance should be redeployed in long run."
                }
            }
    }

    private suspend fun sendClusterStartConsumerCmd(shardId: ShardId) {
        communication.runCatching { trySendShardConsumeCmd(shardId) }
            .onSuccess {
                if (it) {
                    if (options.useVertxCommunication) {
                        logger.info { "Shard $shardId will now be consumed after resharding." }
                    } else {
                        logger.info { "Shard $shardId should now be consumed after resharding. " +
                                "The VCKO Redis cluster communication about remote success is limited, please check for consumer logs to be sure." }
                    }
                } else logger.warn { "No resources in VKCO cluster to consume shard $shardId. Please scale-out VKCO." }
            }.onFailure { logger.warn(it) { "Shard $shardId will NOT be consumed after resharding. Please restart the failing VKCO instance." } }
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

        val childShardIds: ShardIdList = when (reshardingEvent) {
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
        val redisHeimdallOptions: RedisHeimdallOptions,
        val useVertxCommunication: Boolean
    )
}
