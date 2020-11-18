package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import ch.sourcemotion.vertx.redis.client.heimdall.subscription.RedisHeimdallSubscription
import ch.sourcemotion.vertx.redis.client.heimdall.subscription.RedisHeimdallSubscriptionOptions
import ch.sourcemotion.vertx.redis.client.heimdall.subscription.SubscriptionMessage
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import io.vertx.redis.client.Response
import mu.KLogging

internal class RedisOrchestraCommunication(
    private val vertx: Vertx,
    private val redisHeimdallOptions: RedisHeimdallOptions,
    clusterName: OrchestraClusterName,
    consumeShardCommandHandler: Handler<ShardId>
) : OrchestraCommunication(clusterName, consumeShardCommandHandler) {

    private companion object : KLogging()

    private val commandChannelName = "$clusterName/vkco/redis/command/consume/shard"

    private lateinit var redis: Redis
    private lateinit var redisSubscription: RedisHeimdallSubscription

    suspend fun start(): RedisOrchestraCommunication {
        redis = RedisHeimdall.create(vertx, RedisHeimdallOptions(redisHeimdallOptions))
        redisSubscription = RedisHeimdallSubscription.createAwait(
            vertx,
            RedisHeimdallSubscriptionOptions(redisHeimdallOptions),
            this::publishedConsumeShardCmdMessageHandler
        )
        return this
    }

    private fun publishedConsumeShardCmdMessageHandler(msg: SubscriptionMessage) {
        consumeShardCommandHandler.handle(ShardId(msg.message))
    }

    override suspend fun trySendShardConsumeCmd(shardId: ShardId): Boolean {
        // We support to call the instance itself
        return redis.runCatching {
            sendAwait(Request.cmd(Command.PUBLISH).arg(commandChannelName).arg("$shardId")).getReceiverCount()
        }.getOrElse {
            logger.warn(it) {
                "Unable to send consume shard command to another VKCO instance on channel $commandChannelName. " +
                        "You may restart VKCO cluster"
            }
            0
        } > 0
    }

    override suspend fun readyForShardConsumeCommands() {
        redisSubscription.addChannelsAwait(commandChannelName)
    }

    override suspend fun notReadyForShardConsumeCommands() {
        redisSubscription.removeChannelsAwait(commandChannelName)
    }

    private fun Response?.getReceiverCount() = this?.toInteger() ?: 0
}
