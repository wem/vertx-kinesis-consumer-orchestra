package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.EventBusAddr
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.eventbus.ReplyFailure
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.eventbus.unregisterAwait
import mu.KLogging

internal class VertxClusterOrchestraCommunication(
    private val vertx: Vertx,
    clusterName: OrchestraClusterName,
    consumingRequestHandler: Handler<ShardId>
) : OrchestraCommunication(clusterName, consumingRequestHandler) {

    private companion object : KLogging()

    private val eventBusAddr = EventBusAddr(clusterName).resharding.communication

    private var consumeRequestConsumer: MessageConsumer<ShardId>? = null

    override suspend fun trySendShardConsumeCmd(shardId: ShardId) = vertx.eventBus().runCatching {
        requestAwait<Unit>(eventBusAddr.clusterStartConsumerCmd, shardId)
        true
    }.getOrElse {
        // No handler means no instance has free resources
        if (it is ReplyException && it.failureType() == ReplyFailure.NO_HANDLERS) {
            false
        } else {
            logger.warn(it) { "Unable to request for start to consume shard $shardId on another VKCO instance." }
            false
        }
    }

    override suspend fun readyForShardConsumeCommands() {
        vertx.eventBus().consumer<ShardId>(eventBusAddr.clusterStartConsumerCmd).also {
            consumeRequestConsumer = it
            it.handler { msg ->
                runCatching { consumeShardCommandHandler.handle(msg.body()) }
                    .onFailure { cause ->
                        val failureMsg = "Start consumer command handler failed for shard ${msg.body()}"
                        logger.warn(cause) { failureMsg }
                        msg.fail(0, failureMsg)
                    }.onSuccess { msg.ack() }
            }
        }.completionHandlerAwait()
    }

    override suspend fun notReadyForShardConsumeCommands() {
        consumeRequestConsumer?.unregisterAwait()
        consumeRequestConsumer = null
    }
}
