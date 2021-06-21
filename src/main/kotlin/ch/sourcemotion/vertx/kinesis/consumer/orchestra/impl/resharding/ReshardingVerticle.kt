package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StopConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.ReplyException
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange
import software.amazon.awssdk.services.kinesis.model.StreamDescription
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
            .completion().await()
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
    }


    private suspend fun onSplitReshardingEvent(event: SplitReshardingEvent) {
        val (parentShardId, childShardIds) = event
        logger.info { "Received split resharding event. Parent shard: $parentShardId, child shards: $childShardIds" }
        finishShardConsuming(event, parentShardId)
    }

    private suspend fun finishShardConsuming(event: ReshardingEvent,parentShardId: ShardId) {
        val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(options.clusterName.streamName)
        persistChildShardsIterators(event, streamDescription)
        saveFinishedShard(parentShardId, streamDescription)
        sendStopShardConsumerCmd(parentShardId)
    }

    /**
     * A shard get flagged as finished here, as elsewhere it could be too early or to late.
     */
    private suspend fun saveFinishedShard(shardId: ShardId, streamDescription: StreamDescription) {
        // The expiration of the shard finished flag, will be an hour after the shard retention.
        // So it's ensured that we not lose the finished flag of this shard and avoid death data.
        val finishedFlagExpiration = Duration.ofHours(streamDescription.retentionPeriodHours().toLong() + 1).toMillis()
        shardStatePersistence.saveFinishedShard(shardId, finishedFlagExpiration)
        shardStatePersistence.deleteShardSequenceNumber(shardId)
    }

    private suspend fun sendStopShardConsumerCmd(shardId: ShardId) {
        vertx.eventBus()
            .runCatching {
                request<Unit>(
                    EventBusAddr.consumerControl.stopConsumerCmd,
                    StopConsumerCmd(shardId),
                    localOnlyDeliveryOptions
                ).await()
            }
            .onFailure {
                if (it is ReplyException) {
                    when (it.failureCode()) {
                        StopConsumerCmd.CONSUMER_STOP_FAILURE -> logger.warn(it) { "Failed to start consumer for shard \"$shardId\" after resharding." }
                        StopConsumerCmd.UNKNOWN_CONSUMER_FAILURE -> logger.warn(it) { "Consumer of resharded shard \"$shardId\" unknown. This should not happen, but should not be critical." }
                    }
                } else {
                    logger.warn(it) { "Failed to stop consumer for shard \"$shardId\" after resharding. This may relates to a bug. " +
                            "Please restart this VKCO instance and report an issue." }
                }
            }
    }


    /**
     * Persist the starting sequence numbers of resharding child shards.
     *
     * This happens here as this should be early as possible and the follow up workflow steps would be much more easier.
     */
    private suspend fun persistChildShardsIterators(reshardingEvent: ReshardingEvent, streamDescription: StreamDescription) {
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
