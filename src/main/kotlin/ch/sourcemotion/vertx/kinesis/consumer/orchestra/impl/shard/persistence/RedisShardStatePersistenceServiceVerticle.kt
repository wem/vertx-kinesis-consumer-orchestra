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
import io.vertx.redis.client.impl.types.ErrorType
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import kotlin.LazyThreadSafetyMode.NONE

internal class RedisShardStatePersistenceServiceVerticle : CoroutineVerticle(), ShardStatePersistenceService {

    private companion object : KLogging()

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    private val redisKeyFactory by lazy(NONE) {
        RedisKeyFactory(options.applicationName, options.streamName)
    }
    private val redisHeimdallOptions by lazy(NONE) { options.redisHeimdallOptions }

    private var serviceRegistration: MessageConsumer<JsonObject>? = null

    private val redis: Redis by lazy(NONE) { RedisHeimdall.createLight(vertx, redisHeimdallOptions) }

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

    override fun getShardIdsInProgress(shardIds: List<String>, handler: Handler<AsyncResult<List<String>>>) =
        withRetry(handler) {
            val shardIdKeys = shardIds.map { redisKeyFactory.createShardProgressFlagKey(it.asShardIdTyped()) }
            mgetFilter(shardIdKeys, shardIds, 1)
        }

    override fun flagShardInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) = withRetry(handler) {
        val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
        send(
            Request.cmd(Command.SET).arg(key).arg("1").arg("PX").arg(options.shardProgressExpirationMillis)
        ).await()?.okResponseAsBoolean().isTrue()
    }

    override fun flagShardsInProgress(shardIds: List<String>, expirationMillis: Long, handler: Handler<AsyncResult<Boolean>>) = withRetry(handler) {
        val keys = shardIds.map { redisKeyFactory.createShardProgressFlagKey(it.asShardIdTyped()) }
        keys.map { key ->
            async { sendAwait(Request.cmd(Command.SET).arg(key).arg("1").arg("PX").arg(expirationMillis)) }
        }.awaitAll().all { it?.okResponseAsBoolean().isTrue() }
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

    override fun getFinishedShardIds(shardIds: List<String>, handler: Handler<AsyncResult<List<String>>>) =
        withRetry(handler) {
            val shardIdKeys = shardIds.map { redisKeyFactory.createShardFinishedKey(it.asShardIdTyped()) }
            mgetFilter(shardIdKeys, shardIds, 1)
        }

    override fun flagMergeParentReshardingReady(
        parentShardId: String,
        childShardId: String,
        handler: Handler<AsyncResult<Boolean>>
    ) = withRetry(handler) {
        val key = redisKeyFactory.createMergeParentReadyToReshardKey(
            parentShardId.asShardIdTyped(),
            childShardId.asShardIdTyped()
        )
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

    private suspend fun mgetFilter(keys: List<String>, toFilter: List<String>, expectedIntValue: Int) : List<String> {
        if (keys.size != toFilter.size) {
            throw IllegalAccessException("MGET filter expect same count of keys and list of element to filter. " +
                    "keys=${keys.size}, toFilter=${toFilter.size}")
        }
        val cmd = Request.cmd(Command.MGET).apply {
            keys.forEach { shardId -> arg(shardId) }
        }
        val response = redis.sendAwait(cmd)
        return if (response != null) {
            toFilter.filterIndexed { index, _ ->
                response[index]?.toInteger() == expectedIntValue
            }
        } else emptyList()
    }

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
                logger.debug(e) { "Command to Redis failed will retry in $retryDelay millis" }
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

    data class Options(
        val applicationName: String,
        val streamName: String,
        val redisHeimdallOptions: RedisHeimdallOptions,
        val shardProgressExpirationMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS
    )
}

