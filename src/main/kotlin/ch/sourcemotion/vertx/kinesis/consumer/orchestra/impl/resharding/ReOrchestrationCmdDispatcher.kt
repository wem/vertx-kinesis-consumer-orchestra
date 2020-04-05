package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIteratorTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isFalse
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.getShardIteratorAtSequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistence
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.redis.client.connectAwait
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.kotlin.servicediscovery.getRecordsAwait
import io.vertx.kotlin.servicediscovery.publishAwait
import io.vertx.kotlin.servicediscovery.unpublishAwait
import io.vertx.redis.client.*
import io.vertx.servicediscovery.Record
import io.vertx.servicediscovery.ServiceDiscovery
import io.vertx.servicediscovery.types.MessageSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import java.util.*


/**
 * Beside the [ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestrationVerticle] this class is the most central point during resharding.
 * The notifications from shard consumer will arrive here. In the case of merge resharding, both parents needs to be finished before
 * re-orchestration of the shard consumers is initiated. If the shard states are ready, one resharding command dispatcher will notify each (all) other
 * dispatchers. Finally any dispatcher even the one, that initiate the resharding will call [reshardingEventHandler].
 */
abstract class ReOrchestrationCmdDispatcher(
    protected val vertx: Vertx,
    protected val streamName: String,
    private val kinesisClient: KinesisAsyncClient,
    private val shardStatePersistence: ShardStatePersistence,
    protected val scope: CoroutineScope,
    protected val reshardingEventHandler: () -> Unit
) {
    companion object : KLogging() {
        fun create(
            vertx: Vertx,
            applicationName: String,
            streamName: String,
            kinesisClient: KinesisAsyncClient,
            shardStatePersistence: ShardStatePersistence,
            scope: CoroutineScope,
            redisOptions: RedisOptions,
            eventBusBaseDispatching: Boolean = vertx.isClustered,
            reshardingEventHandler: () -> Unit
        ) = if (eventBusBaseDispatching) {
            logger.info { "Create Vert.x event bus based re-orchestration cmd dispatcher" }
            EventBusReOrchestrationCmdDispatcher(
                vertx,
                streamName,
                kinesisClient,
                shardStatePersistence,
                scope,
                reshardingEventHandler
            )
        } else {
            logger.info { "Create Redis based re-orchestration cmd dispatcher" }
            RedisReOrchestrationCmdDispatcher(
                vertx,
                applicationName,
                streamName,
                kinesisClient,
                shardStatePersistence,
                scope,
                redisOptions,
                reshardingEventHandler
            )
        }
    }

    open suspend fun start() {
        vertx.eventBus().consumer(ReshardingEvent.NOTIFICATION_ADDR, this::onConsumerResharding)
    }

    open suspend fun stop() {}

    private fun onConsumerResharding(msg: Message<ReshardingEvent>) {
        scope.launch {
            val reshardingEvent = msg.body()
            if (reshardingEvent is MergeReshardingEvent) {
                // For reliability we safe the child iterator for each finished parent shard.
                // This because if the orchestration will shutdown in the period between both parents did end properly,
                // no shard iterator is available for child shard.
                persistChildShardsIterators(reshardingEvent)
                if (canBeReOrchestrated(reshardingEvent)) {
                    sendReOrchestrateCmd()
                }
            } else {
                // Split can fast forward, as only one running consumer is affected by resharding. No further consumer(s) / shard will be finished
                persistChildShardsIterators(reshardingEvent)
                sendReOrchestrateCmd()
            }
        }
    }

    private suspend fun canBeReOrchestrated(mergeReshardingEvent: MergeReshardingEvent): Boolean {
        val otherParentShardId =
            if (mergeReshardingEvent.adjacentParentShardId == mergeReshardingEvent.finishedShardId) {
                mergeReshardingEvent.parentShardId
            } else {
                mergeReshardingEvent.adjacentParentShardId
            }
        return shardStatePersistence.isShardFinished(otherParentShardId)
    }

    /**
     * Sends a command to re-orchestrate to all organizer, inclusive it self.
     */
    protected abstract suspend fun sendReOrchestrateCmd()

    private suspend fun persistChildShardsIterators(reshardingInfo: ReshardingEvent) {
        val streamDescription = kinesisClient.streamDescriptionWhenActiveAwait(streamName)

        val reshardingChildShardIds: ShardIdList = when (reshardingInfo) {
            is MergeReshardingEvent -> {
                listOf(reshardingInfo.childShardId)
            }
            is SplitReshardingEvent -> {
                reshardingInfo.childShardIds
            }
            else -> throw VertxKinesisConsumerOrchestraException(
                "Resharding of type ${reshardingInfo.reshardingType} is unknown"
            )
        }

        reshardingChildShardIds.forEach { childShardId ->
            val startingSequenceNumber =
                streamDescription.shards().first { it.shardIdTyped() == childShardId }.sequenceNumberRange()
                    .startingSequenceNumber()
            val iterator = kinesisClient.getShardIteratorAtSequenceNumber(
                streamName,
                childShardId,
                startingSequenceNumber
            ).asShardIteratorTyped()
            shardStatePersistence.saveShardIterator(childShardId, iterator)
        }
    }
}

/**
 * Implementation if Vert.x runs in clustered mode. So the re-orchestration command dispatching is working on the Vert.x
 * event bus.
 */
class EventBusReOrchestrationCmdDispatcher(
    vertx: Vertx,
    streamName: String,
    kinesisClient: KinesisAsyncClient,
    shardStatePersistence: ShardStatePersistence,
    scope: CoroutineScope,
    reshardingEventHandler: () -> Unit
) : ReOrchestrationCmdDispatcher(
    vertx,
    streamName,
    kinesisClient,
    shardStatePersistence,
    scope,
    reshardingEventHandler
) {
    private companion object : KLogging() {
        private const val SERVICE_NAME = "resharding-organizer"
    }

    private val serviceDiscovery = ServiceDiscovery.create(vertx)

    private var publishedRecord: Record? = null

    override suspend fun start() {
        super.start()
        val reOrchestrateCmdAddr = "/kinesis-consumer-orchester/cmd/re-orchestrate/${UUID.randomUUID()}"
        vertx.eventBus().consumer(reOrchestrateCmdAddr, this::onReOrchestrateCmd)

        publishedRecord = serviceDiscovery.publishAwait(
            MessageSource.createRecord(SERVICE_NAME, reOrchestrateCmdAddr)
        )
    }

    override suspend fun stop() {
        super.stop()
        publishedRecord?.let { serviceDiscovery.unpublishAwait(it.registration) }
    }

    override suspend fun sendReOrchestrateCmd() {
        serviceDiscovery.getRecordsAwait(jsonObjectOf("name" to SERVICE_NAME)).forEach { serviceRecord ->
            val address = serviceRecord.location.getString(Record.ENDPOINT)
            runCatching { vertx.eventBus().requestAwait<Unit>(address, null) }
                .onSuccess { logger.debug { "Initiated re-orchestration on stream \"$streamName\"" } }
                .onFailure { logger.warn(it) { "Failed to initiate re-orchestration on stream \"$streamName\"" } }
        }
    }

    private fun onReOrchestrateCmd(msg: Message<Unit>) {
        // We acknowledge before resharding
        msg.ack()
        reshardingEventHandler()
    }
}

class RedisReOrchestrationCmdDispatcher(
    vertx: Vertx,
    applicationName: String,
    streamName: String,
    kinesisClient: KinesisAsyncClient,
    shardStatePersistence: ShardStatePersistence,
    scope: CoroutineScope,
    private val redisOptions: RedisOptions,
    reshardingEventHandler: () -> Unit
) : ReOrchestrationCmdDispatcher(
    vertx,
    streamName,
    kinesisClient,
    shardStatePersistence,
    scope,
    reshardingEventHandler
) {

    private companion object : KLogging()

    private lateinit var redis: Redis
    private var running = false
    private val channelName = "resharding-organizer-$applicationName-$streamName"

    override suspend fun start() {
        super.start()
        redis = Redis.createClient(vertx, redisOptions)
        running = true
        subscribeToReOrchestrationCmd(redis.connectAwait())
    }

    override suspend fun stop() {
        super.stop()
        running = false
        runCatching { redis.close() }
    }

    override suspend fun sendReOrchestrateCmd() {
        runCatching { redis.sendAwait(Request.cmd(Command.PUBLISH).arg(channelName).arg("")) }
            .onSuccess { logger.debug { "Initiated re-orchestration on stream \"$streamName\"" } }
            .onFailure { logger.warn(it) { "Failed to initiate re-orchestration on stream \"$streamName\"" } }
    }

    private fun onReOrchestrateCmd(response: Response) {
        runCatching {
            val responseArgs = response.toList()
            if (responseArgs.first().toString().equals("message", true) && responseArgs[1].toString() == channelName) {
                reshardingEventHandler()
            }
        }.onFailure { logger.warn(it) { "Re-orchestration subscription received invalid response $response" } }
    }

    private suspend fun subscribeToReOrchestrationCmd(connection: RedisConnection) {
        connection.handler(this::onReOrchestrateCmd)
        connection.sendAwait(Request.cmd(Command.SUBSCRIBE).arg(channelName))
        reSubscribeOnFailure(connection)
    }

    private fun reSubscribeOnFailure(connection: RedisConnection) {
        connection.exceptionHandler {
            runCatching { connection.close() }
            runCatching {
                scope.launch {
                    var reconnected = false
                    while (reconnected.isFalse()) {
                        reconnected = runCatching { subscribeToReOrchestrationCmd(redis.connectAwait()) }.isSuccess
                        if (reconnected.isFalse()) {
                            val waitFor = 1000L
                            logger.warn { "Unable to refresh Redis pub/sub subscription on re-orchestration command. Will wait for $waitFor millis and retry" }
                            delay(waitFor)
                        }
                    }
                }
            }
        }
    }
}
