package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isTrue
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.DefaultLuaScriptDescription
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallException
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import io.vertx.redis.client.ResponseType
import io.vertx.redis.client.impl.types.ErrorType
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import kotlin.LazyThreadSafetyMode.NONE

internal class RedisShardStatePersistenceServiceVerticle : CoroutineVerticle(), ShardStatePersistenceService {

    private companion object : KLogging()

    private val serviceOptions by lazy(NONE) { config.mapTo(RedisShardStatePersistenceServiceVerticleOptions::class.java) }
    private val redisKeyFactory by lazy(NONE) {
        RedisKeyFactory(
            serviceOptions.applicationName,
            serviceOptions.streamName
        )
    }
    private val redisHeimdallOptions by lazy(NONE) { serviceOptions.redisHeimdallOptions }

    private var serviceRegistration: MessageConsumer<JsonObject>? = null

    private val redis by lazy(NONE) { RedisHeimdall.create(vertx, redisHeimdallOptions) }

    private val luaExecutor by lazy(NONE) { LuaExecutor(redis) }

    private var running: Boolean? = null

    override suspend fun start() {
        running = true
        // This service will register itself
        serviceRegistration = ShardStatePersistenceServiceFactory.expose(vertx, this)
        logger.info { "Kinesis consumer orchestra Redis shard state persistence on duty" }
    }

    override suspend fun stop() {
        running = false
        serviceRegistration?.runCatching { unregister().await() }
        logger.info { "Kinesis consumer orchestra Redis shard state persistence stopped" }
    }

    override fun getShardIdsInProgress(handler: Handler<AsyncResult<List<String>>>) = withRetry(handler) {
            val keyWildcard = redisKeyFactory.createShardProgressFlagKeyWildcard()
            send(Request.cmd(Command.KEYS).arg(keyWildcard)).await()?.map {
                // Remove the key part before the shardid
                extractShardId(it.toString(Charsets.UTF_8))
            } ?: emptyList()
        }

    override fun flagShardInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) = withRetry(handler) {
            val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
            send(
                Request.cmd(Command.SET).arg(key).arg("1").arg("PX")
                    .arg(serviceOptions.shardProgressExpirationMillis.toString())
            ).await()?.okResponseAsBoolean().isTrue()
        }

    override fun flagShardNoMoreInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) =
        withRetry(handler) {
            val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
            send(Request.cmd(Command.DEL).arg(key)).await()?.toInteger() == 1
        }

    override fun saveConsumerShardSequenceNumber(
        shardId: String,
        sequenceNumber: String,
        iteratorPosition: SequenceNumberIteratorPosition,
        handler: Handler<AsyncResult<Void?>>
    ) {
        withRetry(handler) {
            val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
            // We concatenate the sequence number and the iterator type to save data and complexity
            val sequenceNumberState = "${sequenceNumber}-${iteratorPosition.name}"
            val response = send(Request.cmd(Command.SET).arg(sequenceNumberKey).arg(sequenceNumberState)).await()
            val result = response.okResponseAsBoolean()
            if (result.not()) {
                throw VertxKinesisConsumerOrchestraException("Failed to save consumer shard sequence number $sequenceNumberKey")
            }
            null
        }
    }

    override fun getConsumerShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<JsonObject?>>) =
        withRetry(handler) {
            val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
            val response = send(Request.cmd(Command.GET).arg(sequenceNumberKey)).await()
            val sequenceNumberStateRawValue = response?.toString()
            val sequenceNumberState = sequenceNumberStateRawValue?.let { sequenceAndIteratorType ->
                val split = sequenceAndIteratorType.split("-")
                split.first() to SequenceNumberIteratorPosition.valueOf(split.last())
            }
            sequenceNumberState?.let { JsonObject.mapFrom(SequenceNumber(it.first, it.second)) }
        }

    override fun deleteShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<Boolean>>) =
        withRetry(handler) {
            val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
            send(Request.cmd(Command.DEL).arg(sequenceNumberKey)).await()?.toBoolean().isTrue()
        }

    override fun saveFinishedShard(shardId: String, expirationMillis: Long, handler: Handler<AsyncResult<Void?>>) =
        withRetry(handler) {
            val key = redisKeyFactory.createShardFinishedKey(shardId.asShardIdTyped())
            send(Request.cmd(Command.SET).arg(key).arg("1").arg("PX").arg(expirationMillis)).await()
            null
        }

    override fun getFinishedShardIds(handler: Handler<AsyncResult<List<String>>>) = withRetry(handler) {
            val keyWildcard = redisKeyFactory.createShardFinishedRedisKeyWildcard()
            send(Request.cmd(Command.KEYS).arg(keyWildcard)).await()?.let { response ->
                if (response.type() != ResponseType.MULTI) {
                    throw VertxKinesisConsumerOrchestraException(
                        "List of finished shard keys returned unexpected type"
                    )
                }
                response.map { extractShardId(it.toString(Charsets.UTF_8)) }
            } ?: emptyList()
        }

    override fun flagMergeParentReshardingReady(
        parentShardId: String,
        childShardId: String,
        handler: Handler<AsyncResult<Boolean>>
    ) = withRetry(handler) {
        val key = redisKeyFactory.createMergeParentReadyToReshardKey(parentShardId.asShardIdTyped(), childShardId.asShardIdTyped())
        val pattern = redisKeyFactory.createMergeParentReadyToReshardKeyWildcard(childShardId.asShardIdTyped())
        luaExecutor.execute(
            DefaultLuaScriptDescription.SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN,
            listOf(key),
            listOf("1", pattern)
        )?.toInteger() == 2
    }

    override fun deleteMergeParentsReshardingReadyFlag(childShardId: String, handler: Handler<AsyncResult<Int>>) =
        withRetry(handler) {
            val pattern = redisKeyFactory.createMergeParentReadyToReshardKeyWildcard(childShardId.asShardIdTyped())
            luaExecutor.execute(
                DefaultLuaScriptDescription.DELETE_VALUES_BY_KEY_PATTERN_RETURN_DELETED_COUNT,
                listOf(),
                listOf(pattern)
            )?.toInteger() ?: 0
        }

    private fun extractShardId(keyContainsShardId: String) =
        "shardId-${keyContainsShardId.substringAfter("shardId-")}"

    private fun <T> withRetry(handler: Handler<AsyncResult<T>>, task: suspend Redis.() -> T) {
        launch {
            withRetrySuspend(handler, task)
        }
    }

    private suspend fun <T> withRetrySuspend(handler: Handler<AsyncResult<T>>, task: suspend Redis.() -> T) {
        try {
            val result = redis.task()
            handler.handle(Future.succeededFuture(result))
        } catch (e: RedisHeimdallException) {
            if (running.isTrue()) {
                val retryDelay =
                    redisHeimdallOptions.reconnectInterval / 2 // We try more often as the reconnect interval as we don't known when the connection was lost.
                logger.debug { "Command to Redis failed will retry in $retryDelay millis" }
                delay(retryDelay)
                withRetrySuspend(handler, task)
            } else {
                logger.info { "Skip Redis command retry because shard persistence was stopped" }
                handler.handle(Future.failedFuture(IllegalStateException("Command skipped because shard iterator persistence is no more running")))
            }
        } catch (e: ErrorType) {
            handler.handle(Future.failedFuture(e))
        } catch (e: Exception) {
            logger.warn(e) { "Unexpected error type" }
            handler.handle(Future.failedFuture(VertxKinesisConsumerOrchestraException(cause = e)))
        }
    }
}

data class RedisShardStatePersistenceServiceVerticleOptions(
    val applicationName: String,
    val streamName: String,
    val redisHeimdallOptions: RedisHeimdallOptions,
    val shardProgressExpirationMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS,
)
