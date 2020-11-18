package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.Handler
import io.vertx.core.Vertx

/**
 * Provides the communcation between VKCO cluster instances. All things about this kind of communication should happen
 * here
 */
internal abstract class OrchestraCommunication(
    protected val clusterName: OrchestraClusterName,
    protected val consumeShardCommandHandler: Handler<ShardId>
) {
    companion object {
        suspend fun create(
            vertx: Vertx,
            redisHeimdallOptions: RedisHeimdallOptions,
            clusterName: OrchestraClusterName,
            vertxCommunication: Boolean = vertx.isClustered,
            consumingRequestHandler: Handler<ShardId>
        ) = if (vertxCommunication) {
            VertxClusterOrchestraCommunication(vertx, clusterName, consumingRequestHandler)
        } else {
            RedisOrchestraCommunication(vertx, redisHeimdallOptions, clusterName, consumingRequestHandler).start()
        }
    }

    /**
     * Requests shard consuming on another instance. Will return if the request was accepted.
     */
    abstract suspend fun trySendShardConsumeCmd(shardId: ShardId): Boolean

    /**
     * Publishes this instance as ready to consume from arbitrary shard on request.
     */
    abstract suspend fun readyForShardConsumeCommands()

    /**
     * This instance will not longer accept consuming requests.
     */
    abstract suspend fun notReadyForShardConsumeCommands()
}

