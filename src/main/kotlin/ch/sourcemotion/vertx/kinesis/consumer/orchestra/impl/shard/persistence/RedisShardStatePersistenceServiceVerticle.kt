package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isTrue
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
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
import io.vertx.kotlin.core.eventbus.unregisterAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.*
import io.vertx.redis.client.impl.types.ErrorType
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Duration
import java.util.*
import kotlin.LazyThreadSafetyMode.NONE

class RedisShardStatePersistenceServiceVerticle : CoroutineVerticle(), ShardStatePersistenceService {

    private companion object : KLogging()

    private val serviceOptions by lazy(NONE) { config.mapTo(RedisShardStatePersistenceServiceVerticleOptions::class.java) }
    private val redisKeyFactory by lazy(NONE) {
        RedisKeyFactory(
            serviceOptions.applicationName,
            serviceOptions.streamName
        )
    }
    private val redisHeimdallOptions by lazy(NONE) {
        RedisHeimdallOptions(serviceOptions.redisOptions).apply {
            reconnectingNotifications = false
            reconnectInterval = serviceOptions.reconnectIntervalMillis
        }
    }

    private var serviceRegistration: MessageConsumer<JsonObject>? = null

    private val redis by lazy(NONE) { RedisHeimdall.create(vertx, redisHeimdallOptions) }

    private val keepAliveTimerIds = Collections.synchronizedMap(mutableMapOf<String, Long>())

    private var running: Boolean? = null

    override suspend fun start() {
        running = true
        // This service will register itself
        serviceRegistration = ShardStatePersistenceServiceFactory.expose(vertx, this)
        logger.info { "Kinesis consumer orchestra Redis shard state persistence on duty" }
    }

    override suspend fun stop() {
        running = false
        serviceRegistration?.runCatching { unregisterAwait() }
        runCatching { cancelProgressKeepAlive() }
        logger.info { "Kinesis consumer orchestra Redis shard state persistence stopped" }
    }

    private fun cancelProgressKeepAlive(shardId: String? = null) {
        val filteredTimers = if (shardId.isNotNullOrBlank()) {
            keepAliveTimerIds.filter { it.key == shardId }
        } else {
            keepAliveTimerIds
        }
        filteredTimers.forEach {
            runCatching { vertx.cancelTimer(it.value) }
            keepAliveTimerIds.remove(it.key)
        }
    }

    override fun getShardIdsInProgress(handler: Handler<AsyncResult<List<String>>>) {
        withRetry(handler) {
            val keyWildcard = redisKeyFactory.createShardProgressFlagKeyWildcard()
            sendAwait(Request.cmd(Command.KEYS).arg(keyWildcard))?.map {
                // Remove the key part before the shardid
                extractShardId(it.toString(Charsets.UTF_8))
            } ?: emptyList()
        }
    }

    override fun flagShardInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) {
        withRetry(handler) {
            val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
            sendAwait(
                Request.cmd(Command.SET).arg(key).arg("1").arg("PX")
                    .arg(serviceOptions.shardProgressExpirationMillis.toString())
            )?.okResponseAsBoolean().isTrue()
        }
    }

    override fun flagShardNoMoreInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) {
        withRetry(handler) {
            cancelProgressKeepAlive(shardId)
            val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
            sendAwait(Request.cmd(Command.DEL).arg(key))?.toInteger() == 1
        }
    }

    override fun startShardProgressAndKeepAlive(shardId: String, handler: Handler<AsyncResult<Void?>>) {
        withRetry(handler) {
            val keepAliveInterval =
                Duration.ofMillis(serviceOptions.shardProgressExpirationMillis).dividedBy(2).toMillis()
            flagShardInProgress(shardId, Handler {
                // Make sure keep alive updated enough early / often
                val keepAliveTimerId = vertx.setPeriodic(keepAliveInterval) {
                    flagShardInProgress(shardId, Handler { })
                }
                keepAliveTimerIds[shardId] = keepAliveTimerId
            })
            null
        }
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
            val response = sendAwait(Request.cmd(Command.SET).arg(sequenceNumberKey).arg(sequenceNumberState))
            val result = response.okResponseAsBoolean()
            if (result.not()) {
                throw VertxKinesisConsumerOrchestraException("Failed to save consumer shard sequence number $sequenceNumberKey")
            }
            null
        }
    }

    override fun getConsumerShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<JsonObject?>>) {
        withRetry(handler) {
            val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
            val response = sendAwait(Request.cmd(Command.GET).arg(sequenceNumberKey))
            val sequenceNumberStateRawValue = response?.toString()
            val sequenceNumberState = sequenceNumberStateRawValue?.let { sequenceAndIteratorType ->
                val split = sequenceAndIteratorType.split("-")
                split.first() to SequenceNumberIteratorPosition.valueOf(split.last())
            }
            sequenceNumberState?.let { JsonObject.mapFrom(SequenceNumber(it.first, it.second)) }
        }
    }

    override fun deleteShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<Boolean>>) {
        withRetry(handler) {
            val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
            sendAwait(Request.cmd(Command.DEL).arg(sequenceNumberKey))?.toBoolean().isTrue()
        }
    }

    override fun saveFinishedShard(shardId: String, expirationMillis: Long, handler: Handler<AsyncResult<Void?>>) {
        withRetry(handler) {
            val key = redisKeyFactory.createShardFinishedKey(shardId.asShardIdTyped())
            sendAwait(
                Request.cmd(Command.SET).arg(key).arg("1").arg("PX")
                    .arg(serviceOptions.shardProgressExpirationMillis.toString())
            )
            null
        }
    }

    override fun getFinishedShardIds(handler: Handler<AsyncResult<List<String>>>) {
        withRetry(handler) {
            val keyWildcard = redisKeyFactory.createShardFinishedRedisKeyWildcard()
            sendAwait(Request.cmd(Command.KEYS).arg(keyWildcard))?.let { response ->
                if (response.type() != ResponseType.MULTI) {
                    throw VertxKinesisConsumerOrchestraException(
                        "List of finished shard keys returned unexpected type"
                    )
                }
                response.map { extractShardId(it.toString(Charsets.UTF_8)) }
            } ?: emptyList()
        }
    }

    var commandCounter = 0

    override fun incrAndGetMergeReshardingEventCount(childShardId: String, handler: Handler<AsyncResult<Int>>) {
        logger.info { "Received command number ${++commandCounter}" }
        withRetry(handler) {
            val counterKey = redisKeyFactory.createMergeReshardingEventCountKey(childShardId.asShardIdTyped())
            sendAwait(Request.cmd(Command.INCR).arg(counterKey))?.toInteger()
                ?: throw VertxKinesisConsumerOrchestraException(
                    "Unable to increment merge resharding event count of child shard $childShardId"
                ).also { logger.error { "Unable to increment merge resharding event count of child shard $childShardId" } }

        }
    }

    override fun deleteMergeReshardingEventCount(childShardId: String, handler: Handler<AsyncResult<Void?>>) {
        withRetry(handler) {
            val counterKey = redisKeyFactory.createMergeReshardingEventCountKey(childShardId.asShardIdTyped())
            sendAwait(Request.cmd(Command.DEL).arg(counterKey))?.let { response ->
                response.runCatching {
                    if (toInteger() != 1) {
                        logger.warn { "Unable to delete merge resharding event counter of child shard $childShardId" }
                    }
                }
            }
            null
        }
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
    val redisOptions: RedisOptions,
    val shardProgressExpirationMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS,
    val reconnectIntervalMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS
)
