package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isTrue
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import io.vertx.core.Vertx
import io.vertx.kotlin.redis.client.*
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.ResponseType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.io.Closeable
import java.time.Duration

class ShardStatePersistence(
    @PublishedApi
    internal val redisApi: RedisAPI,
    private val shardProgressExpiration: Duration,
    private val redisKeyFactory: RedisKeyFactory
) : Closeable {
    suspend fun isShardInProgress(shardId: ShardId): Boolean {
        val key = redisKeyFactory.createShardProgressFlagKey(shardId)
        return redisApi.getAwait(key)?.toBoolean().isTrue()
    }

    suspend fun getShardIdsInProgress(): ShardIdList {
        val keyWildcard = redisKeyFactory.createShardProgressFlagKeyWildcard()
        return redisApi.keysAwait(keyWildcard)?.map {
            // Remove the key part before the shardid
            val fullKey = it.toString(Charsets.UTF_8)
            extractTypedShardId(fullKey)
        } ?: emptyList()
    }

    suspend fun flagShardNoMoreInProgress(shardId: ShardId): Boolean {
        val key = redisKeyFactory.createShardProgressFlagKey(shardId)
        return redisApi.delAwait(listOf(key))?.toInteger() == 1
    }

    suspend fun flagShardInProgress(shardId: ShardId): Boolean {
        val key = redisKeyFactory.createShardProgressFlagKey(shardId)
        return redisApi.setAwait(listOf(key, "1", "PX", shardProgressExpiration.toMillis().toString()))
            ?.okResponseAsBoolean()
            .isTrue()
    }

    suspend fun startShardProgressAndKeepAlive(vertx: Vertx, scope: CoroutineScope, shardId: ShardId): TimerId {
        flagShardInProgress(shardId)
        // Make sure keep alive updated enough early / often
        val keepAliveInterval = shardProgressExpiration.dividedBy(2).toMillis()
        return vertx.setPeriodic(keepAliveInterval) {
            scope.launch { flagShardInProgress(shardId) }
        }.asTimerIdTyped()
    }

    suspend fun saveShardIterator(shardId: ShardId, shardIterator: ShardIterator): ShardIterator {
        val key = redisKeyFactory.createShardIteratorKey(shardId)
        redisApi.setAwait(listOf(key, shardIterator.iter))
        return shardIterator
    }

    suspend fun getShardIterator(shardId: ShardId): ShardIterator? {
        val key = redisKeyFactory.createShardIteratorKey(shardId)
        return redisApi.getAwait(key)?.toString(Charsets.UTF_8)?.asShardIteratorTyped()
    }

    suspend fun deleteShardIterator(shardId: ShardId): Boolean {
        val key = redisKeyFactory.createShardIteratorKey(shardId)
        return redisApi.delAwait(listOf(key))?.toBoolean().isTrue()
    }

    suspend fun saveFinishedShard(shardId: ShardId, expirationMillis: Long) {
        val key = redisKeyFactory.createShardFinishedKey(shardId)
        redisApi.setAwait(listOf(key, "1", "PX", expirationMillis.toString()))
    }

    suspend fun isShardFinished(shardId: ShardId): Boolean {
        val key = redisKeyFactory.createShardFinishedKey(shardId)
        return redisApi.existsAwait(listOf(key))?.toBoolean().isTrue()
    }

    suspend fun getFinishedShardIds(): ShardIdList {
        val keyWildcard = redisKeyFactory.createShardFinishedRedisKeyWildcard()
        return redisApi.keysAwait(keyWildcard)?.let { response ->
            if (response.type() != ResponseType.MULTI) {
                throw VertxKinesisConsumerOrchestraException(
                    "List of finished shard keys returned unexpected type"
                )
            }
            response.map { extractTypedShardId(it.toString(Charsets.UTF_8)) }
        } ?: emptyList()
    }

    private fun extractTypedShardId(keyContainsShardId: String) =
        "shardId-${keyContainsShardId.substringAfter("shardId-")}".asShardIdTyped()

    override fun close() {
        redisApi.close()
    }
}

