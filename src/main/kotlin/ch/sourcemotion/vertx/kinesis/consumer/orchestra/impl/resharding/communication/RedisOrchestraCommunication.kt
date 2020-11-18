package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
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
import java.util.*

internal class RedisOrchestraCommunication(
    private val vertx: Vertx,
    private val redisHeimdallOptions: RedisHeimdallOptions,
    clusterName: OrchestraClusterName,
    consumeShardCommandHandler: Handler<ShardId>
) : OrchestraCommunication(clusterName, consumeShardCommandHandler) {

    private companion object : KLogging()

    private val commandSubscriptionsMap = "$clusterName/vkco/redis/command/consume/shard/subscriptions"
    private val commandChannelName = "$clusterName/vkco/redis/command/consume/shard/${UUID.randomUUID()}"

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
        val channelName = popCommandChannelName()

        // We support to call the instance itself

        return if (channelName.isNotNullOrBlank()) {
            redis.runCatching {
                sendAwait(
                    Request.cmd(Command.PUBLISH).arg(channelName).arg("$shardId")
                ).getReceiverCount()
            }.getOrElse {
                logger.warn(it) {
                    "Unable to send consume shard command to another VKCO instance on channel $channelName. " +
                            "You may restart VKCO cluster"
                }
                0
            } > 0
        } else false
    }

    private suspend fun popCommandChannelName() = redis.runCatching {
        sendAwait(Request.cmd(Command.SPOP).arg(commandSubscriptionsMap))?.toString()
    }.getOrElse {
        logger.warn(it) {
            "Unable to get ready for consume shard command subscription address of another VKCO instance. Looks like no other VKCO instance has free capacity to consumer" +
                    "an additional shard. Please scale out VKCO"
        }
        null
    }

    override suspend fun readyForShardConsumeCommands() {
        if (addChannelNameToSubscriptionMap()) {
            redisSubscription.addChannelsAwait(commandChannelName)
        } else {
            logger.warn { "Failed to make this VKCO instance ready for consume shard command." }
        }
    }

    override suspend fun notReadyForShardConsumeCommands() {
        redisSubscription.removeChannelsAwait(commandChannelName)
        removeChannelNameFromSubscriptionMap()
    }

    private fun Response?.getReceiverCount() = this?.toInteger() ?: 0

    private suspend fun addChannelNameToSubscriptionMap() = redis.runCatching {
        sendAwait(
            Request.cmd(Command.SADD).arg(commandSubscriptionsMap)
                .arg(commandChannelName)
        )
        true
    }.getOrElse {
        logger.warn(it) { "Unable to add subscription channel name $commandChannelName to subscription map of " }
        false
    }

    private suspend fun removeChannelNameFromSubscriptionMap() = redis.runCatching {
        sendAwait(
            Request.cmd(Command.SREM).arg(commandSubscriptionsMap)
                .arg(commandChannelName)
        )
        true
    }.getOrElse {
        logger.warn(it) { "Unable to add subscription channel name $commandChannelName to subscription map of " }
        false
    }
}
