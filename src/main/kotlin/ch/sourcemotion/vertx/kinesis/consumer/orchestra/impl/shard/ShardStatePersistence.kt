package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isFalse
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isTrue
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import io.vertx.core.Vertx
import io.vertx.kotlin.redis.client.*
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.ResponseType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import mu.KLogging
import java.io.Closeable
import java.time.Duration

class ShardStatePersistence(
    @PublishedApi
    internal val redisApi: RedisAPI,
    private val shardProgressExpiration: Duration,
    private val redisKeyFactory: RedisKeyFactory
) : Closeable {

    private companion object : KLogging()

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

    suspend fun saveConsumerShardSequenceNumber(
        shardId: ShardId,
        sequenceNumber: SequenceNumber
    ): SequenceNumber {
        val sequenceNumberKey = redisKeyFactory.createShardSequenceInfoKey(shardId)
        // We concatenate the sequence number and the iterator type to save data and complexity
        val sequenceNumberState = "${sequenceNumber.number}-${sequenceNumber.iteratorPosition.name}"
        val success = redisApi.setAwait(listOf(sequenceNumberKey, sequenceNumberState)).okResponseAsBoolean()
        if (success.isFalse()) {
            logger.warn { "Unable to save consumer sequence number info: \"$sequenceNumberState\" on shard: \"$shardId\"" }
        }
        return sequenceNumber
    }

    suspend fun getConsumerShardSequenceNumber(shardId: ShardId): SequenceNumber? {
        val sequenceNumberKey = redisKeyFactory.createShardSequenceInfoKey(shardId)
        val response = redisApi.getAwait(sequenceNumberKey)
        val sequenceNumberStateRawValue = response?.toString()
        val sequenceNumberState = sequenceNumberStateRawValue?.let { sequenceAndIteratorType ->
            val split = sequenceAndIteratorType.split("-")
            split.first() to SequenceNumberIteratorPosition.valueOf(split.last())
        }

        return sequenceNumberState?.let { SequenceNumber(it.first, it.second) }
    }

    suspend fun deleteShardSequenceNumber(shardId: ShardId): Boolean {
        val iteratorKey = redisKeyFactory.createShardSequenceInfoKey(shardId)
        val sequenceNumberKey = redisKeyFactory.createShardSequenceInfoKey(shardId)
        return redisApi.delAwait(listOf(iteratorKey, sequenceNumberKey))?.toBoolean().isTrue()
    }

    suspend fun saveFinishedShard(shardId: ShardId, expirationMillis: Long) {
        val key = redisKeyFactory.createShardFinishedKey(shardId)
        redisApi.setAwait(listOf(key, "1", "PX", expirationMillis.toString()))
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

    suspend fun getMergeReshardingEventCount(childShardId: ShardId): Int {
        val counterKey = redisKeyFactory.createMergeReshardingEventCountKey(childShardId)
        return redisApi.incrAwait(counterKey)?.toInteger() ?: 0
    }

    suspend fun deleteMergeReshardingEventCount(childShardId: ShardId) {
        val counterKey = redisKeyFactory.createMergeReshardingEventCountKey(childShardId)
        redisApi.delAwait(listOf(counterKey))?.let { response ->
            response.runCatching {
                if (toInteger() != 1) {
                    logger.warn { "Unable to delete merge resharding event counter of child shard $childShardId" }
                }
            }
        }
    }

    private fun extractTypedShardId(keyContainsShardId: String) =
        "shardId-${keyContainsShardId.substringAfter("shardId-")}".asShardIdTyped()

    override fun close() {
        redisApi.close()
    }
}

