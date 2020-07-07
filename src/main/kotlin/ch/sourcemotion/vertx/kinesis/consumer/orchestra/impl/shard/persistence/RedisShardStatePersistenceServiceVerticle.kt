package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumberIteratorPosition
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isTrue
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.setTimerAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.unregisterAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.redis.client.connectAwait
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.*
import io.vertx.redis.client.impl.types.ErrorType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import mu.KLogging
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

private typealias RedisCommand = suspend () -> Unit

class RedisShardStatePersistenceServiceVerticle : CoroutineVerticle(), ShardStatePersistenceService {

    private companion object : KLogging()

    private val serviceOptions by lazy { config.mapTo(RedisShardStatePersistenceServiceVerticleOptions::class.java) }
    private val redisKeyFactory by lazy { RedisKeyFactory(serviceOptions.applicationName, serviceOptions.streamName) }

    private val pendingCommands = ConcurrentLinkedQueue<RedisCommand>()

    @Volatile
    private lateinit var redisConnection: RedisConnection

    @Volatile
    private var connected = false

    private var serviceRegistration: MessageConsumer<JsonObject>? = null

    private val keepAliveTimerIds = Collections.synchronizedMap(mutableMapOf<String, Long>())

    private val inFlightJobs = Collections.synchronizedMap(mutableMapOf<RedisCommand, Job>())

    override suspend fun start() {
        runCatching { connectToRedis() }
            .onFailure { cause -> attemptReconnect(initialConnectivity = true, cause = cause) }

        // This service will register itself
        serviceRegistration = ShardStatePersistenceServiceFactory.expose(vertx, this)

        logger.info { "Kinesis consumer orchestra Redis shard state persistence on duty" }
    }

    override suspend fun stop() {
        logger.info { "Kinesis consumer orchestra Redis shard state persistence get stopped" }
        serviceRegistration?.runCatching { unregisterAwait() }
        runCatching { cancelProgressKeepAlive() }
        runCatching { redisConnection.close() }
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
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                val keyWildcard = redisKeyFactory.createShardProgressFlagKeyWildcard()
                sendAwait(Request.cmd(Command.KEYS).arg(keyWildcard))?.map {
                    // Remove the key part before the shardid
                    val fullKey = it.toString(Charsets.UTF_8)
                    extractShardId(fullKey)
                } ?: emptyList()
            }
        }
    }

    override fun flagShardInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
                sendAwait(
                    Request.cmd(Command.SET).arg(key).arg("1").arg("PX")
                        .arg(serviceOptions.shardProgressExpirationMillis.toString())
                )?.okResponseAsBoolean().isTrue()
            }
        }
    }

    override fun flagShardNoMoreInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                cancelProgressKeepAlive(shardId)
                val key = redisKeyFactory.createShardProgressFlagKey(shardId.asShardIdTyped())
                sendAwait(Request.cmd(Command.DEL).arg(key))?.toInteger() == 1
            }
        }
    }

    override fun startShardProgressAndKeepAlive(shardId: String, handler: Handler<AsyncResult<Void?>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
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
    }

    override fun saveConsumerShardSequenceNumber(
        shardId: String,
        sequenceNumber: String,
        iteratorPosition: SequenceNumberIteratorPosition,
        handler: Handler<AsyncResult<Void?>>
    ) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
                // We concatenate the sequence number and the iterator type to save data and complexity
                val sequenceNumberState = "${sequenceNumber}-${iteratorPosition.name}"
                val response =
                    redisConnection.sendAwait(Request.cmd(Command.SET).arg(sequenceNumberKey).arg(sequenceNumberState))
                val result = response.okResponseAsBoolean()
                if (result.not()) {
                    throw VertxKinesisConsumerOrchestraException("Failed to save consumer shard sequence number $sequenceNumberKey")
                }
                null
            }
        }
    }

    override fun getConsumerShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<JsonObject?>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
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
    }

    override fun deleteShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<Boolean>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                val sequenceNumberKey = redisKeyFactory.createShardSequenceNumberKey(shardId.asShardIdTyped())
                sendAwait(Request.cmd(Command.DEL).arg(sequenceNumberKey))?.toBoolean().isTrue()
            }
        }
    }

    override fun saveFinishedShard(shardId: String, expirationMillis: Long, handler: Handler<AsyncResult<Void?>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                val key = redisKeyFactory.createShardFinishedKey(shardId.asShardIdTyped())
                sendAwait(
                    Request.cmd(Command.SET).arg(key).arg("1").arg("PX")
                        .arg(serviceOptions.shardProgressExpirationMillis.toString())
                )
                null
            }
        }
    }

    override fun getFinishedShardIds(handler: Handler<AsyncResult<List<String>>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
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
    }

    override fun incrAndGetMergeReshardingEventCount(childShardId: String, handler: Handler<AsyncResult<Int>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
                val counterKey = redisKeyFactory.createMergeReshardingEventCountKey(childShardId.asShardIdTyped())
                sendAwait(Request.cmd(Command.INCR).arg(counterKey))?.toInteger()
                    ?: throw VertxKinesisConsumerOrchestraException(
                        "Unable to increment merge resharding event count of child shard $childShardId"
                    )
            }
        }
    }

    override fun deleteMergeReshardingEventCount(childShardId: String, handler: Handler<AsyncResult<Void?>>) {
        callOrQueueCommand {
            redisConnection.withHandlerReply(handler) {
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
    }

    /**
     * Wraps and command against Redis. In the case of a connection related failure, the command will added to the queue
     * and reconnect will be started.
     */
    private fun callOrQueueCommand(retry: Boolean = false, command: RedisCommand) {
        if (connected || retry) {
            val job = launch {
                val commandResult = runCatching { command() }
                inFlightJobs.remove(command)
                commandResult.onFailure { throwable ->
                    if (throwable.isConnectionClosedFailure()) {
                        addCommandToQueue(command)
                        attemptReconnect(cause = throwable)
                    } else {
                        logger.error(throwable) { "Unable to handle this kind of failure" }
                    }
                }
            }
            inFlightJobs[command] = job
        } else {
            addCommandToQueue(command)
        }
    }

    /**
     * Connects to Redis. Afterwards the connection will be validated and if anything was successful the pending
     * command get (re)executed.
     */
    private suspend fun connectToRedis() {
        val client = Redis.createClient(vertx, serviceOptions.redisOptions)

        val connection = client.connectAwait()

        // If an network related exception was thrown, the inflight jobs are migrated to pending commands
        connection.exceptionHandler {
            launch {
                attemptReconnect {
                    inFlightJobs.keys.forEach { addCommandToQueue(it) }
                    inFlightJobs.values.forEach { job ->
                        job.runCatching { cancel(CancelInFlightCommandException()) }
                    }
                    inFlightJobs.clear()
                }
            }
        }

        connection.verifyConnection()
        redisConnection = connection

        logger.info { "Connected to Redis" }

        executePendingCommands()
        connected = true
    }

    /**
     * Recursively attempt to reconnect. This function will call itself if the reconnect did fail.
     */
    private suspend fun attemptReconnect(
        initialConnectivity: Boolean = false,
        attempt: Int = 1,
        cause: Throwable? = null,
        /**
         * Will be called after shard state persistence got flagged disconnected.
         */
        flaggedDisconnectedCallback: (() -> Unit)? = null
    ) {
        // Avoid multiple parallel reconnection attempts
        if (connected.not() && attempt == 1) {
            return
        }
        connected = false
        flaggedDisconnectedCallback?.invoke()
        if (initialConnectivity) {
            logger.warn(cause) { "Initial connection to Redis not established. Will try to reconnect in \"${serviceOptions.reconnectIntervalMillis}\" millis" }
        } else if (attempt == 1) {
            logger.warn(cause) { "Connection to Redis lost. Will try to reconnect in \"${serviceOptions.reconnectIntervalMillis}\" millis" }
        }
        vertx.setTimerAwait(serviceOptions.reconnectIntervalMillis)
        runCatching { connectToRedis() }.onFailure {
            logger.warn(it) { "Unable to reconnect to Redis in \"$attempt\". Will retry in \"${serviceOptions.reconnectIntervalMillis}\" millis" }
            attemptReconnect(attempt = attempt + 1)
        }
    }

    private fun executePendingCommands() {
        logger.debug { "Call ${pendingCommands.size} pending commands" }
        while (pendingCommands.isNotEmpty()) {
            val nextCmd = pendingCommands.poll()
            runCatching { callOrQueueCommand(true, nextCmd) }.onFailure { addCommandToQueue(nextCmd) }
        }
    }

    private fun addCommandToQueue(command: RedisCommand) {
        if (pendingCommands.contains(command).not()) {
            pendingCommands.offer(command)
            logger.debug { "Command added to queue. Currently ${pendingCommands.size} pending commands" }
        }
    }

    private fun extractShardId(keyContainsShardId: String) =
        "shardId-${keyContainsShardId.substringAfter("shardId-")}"

    private inline fun <R> RedisConnection.withHandlerReply(
        handler: Handler<AsyncResult<R>>,
        block: RedisConnection.() -> R
    ) {
        runCatching(block)
            .onSuccess { handler.handle(Future.succeededFuture(it)) }
            .onFailure { throwable ->
                if (throwable.isConnectionClosedFailure()) {
                    throw throwable
                } else if (throwable !is CancelInFlightCommandException) {
                    handler.handle(Future.failedFuture(throwable))
                }
            }
    }

    private fun Throwable.isConnectionClosedFailure() = this is ErrorType && this.kind == "CONNECTION_CLOSED" ||
            this is IOException

    private suspend fun RedisConnection.verifyConnection() =
        if (sendAwait(Request.cmd(Command.PING))?.toString() == "PONG") {
            true
        } else throw RedisConnectionVerificationException()
}

private class RedisConnectionVerificationException : Exception()

private class CancelInFlightCommandException : CancellationException()

data class RedisShardStatePersistenceServiceVerticleOptions(
    val applicationName: String,
    val streamName: String,
    val redisOptions: RedisOptions,
    val shardProgressExpirationMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS,
    val reconnectIntervalMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS
)
