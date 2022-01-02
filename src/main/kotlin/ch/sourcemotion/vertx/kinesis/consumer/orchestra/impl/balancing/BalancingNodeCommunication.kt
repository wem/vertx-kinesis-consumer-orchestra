package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterNodeId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StopConsumersCmdResult
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallLight
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import ch.sourcemotion.vertx.redis.client.heimdall.subscription.RedisHeimdallSubscription
import ch.sourcemotion.vertx.redis.client.heimdall.subscription.RedisHeimdallSubscriptionOptions
import ch.sourcemotion.vertx.redis.client.heimdall.subscription.SubscriptionMessage
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import mu.KLogging
import java.util.*

/**
 * Inter VKCO node communication for shard consumer balancing.
 */
interface BalancingNodeCommunication {
    suspend fun start(): BalancingNodeCommunication
    fun sendStopConsumersCmd(
        destinationNodeId: OrchestraClusterNodeId,
        consumerCount: Int
    ): Future<StopConsumersCmdResult>

    fun sendStartConsumersCmd(destinationNodeId: OrchestraClusterNodeId, shardIds: List<ShardId>): Future<Int>
}

class RedisBalancingNodeCommunication(
    private val vertx: Vertx,
    private val redisOptions: RedisHeimdallOptions,
    private val nodeId: OrchestraClusterNodeId,
    private val balancingCommandDurationThresholdMillis: Long,
    private val stopConsumersCmdHandler: StopConsumersCmdHandler,
    private val startConsumersCmdHandler: StartConsumersCmdHandler,
) : BalancingNodeCommunication {

    private companion object : KLogging()

    private val pendingStopCmdResponses = HashMap<CmdId, Promise<StopConsumersCmdResult>>()
    private val pendingStartCmdResponses = HashMap<CmdId, Promise<Int>>()

    private val startConsumersCmdChannelName = startConsumersCmdChannelNameOf(nodeId)
    private val stopConsumersCmdChannelName = stopConsumersCmdChannelNameOf(nodeId)

    private val startConsumersCmdResponseChannelName = "/balancing/start-consumers/$nodeId/response"
    private val stopConsumersCmdResponseChannelName = "/balancing/stop-consumers/$nodeId/response"

    private lateinit var subscription: RedisHeimdallSubscription
    private val sender: RedisHeimdallLight = RedisHeimdallLight(vertx, redisOptions)

    private fun startConsumersCmdChannelNameOf(nodeId: OrchestraClusterNodeId) = "/balancing/start-consumers/${nodeId}"
    private fun stopConsumersCmdChannelNameOf(nodeId: OrchestraClusterNodeId) = "/balancing/stop-consumers/${nodeId}"

    override suspend fun start(): BalancingNodeCommunication {
        subscription = RedisHeimdallSubscription.create(
            vertx,
            RedisHeimdallSubscriptionOptions(redisOptions).addChannelNames(
                startConsumersCmdChannelName,
                stopConsumersCmdChannelName,
                startConsumersCmdResponseChannelName,
                stopConsumersCmdResponseChannelName
            ),
            ::onCmdOrResponse
        ).await()
        logger.info { "Balancing communication started on VKCO node $nodeId" }
        return this
    }

    private fun onCmdOrResponse(msg: SubscriptionMessage) {
        when (msg.channel) {
            startConsumersCmdChannelName -> {
                val cmd = JsonObject(msg.message).mapTo(StartConsumersBalancingCmd::class.java)
                handleStartConsumersCmd(cmd)
            }
            stopConsumersCmdChannelName -> {
                val cmd = JsonObject(msg.message).mapTo(StopConsumersBalancingCmd::class.java)
                handleStopConsumersCmd(cmd)
            }
            startConsumersCmdResponseChannelName -> {
                val response = JsonObject(msg.message).mapTo(BalancingCmdResponse::class.java)
                handleStartConsumersCmdResponse(response)
            }
            stopConsumersCmdResponseChannelName -> {
                val response = JsonObject(msg.message).mapTo(BalancingCmdResponse::class.java)
                handleStopConsumersCmdResponse(response)
            }
        }
    }

    private fun handleStartConsumersCmd(cmd: StartConsumersBalancingCmd) {
        startConsumersCmdHandler.handle(cmd.shardsToStart)
            .onSuccess { activeConsumers ->
                publishResponse(cmd, StartConsumersBalancingCmdResponse(activeConsumers, cmd.cmdId))
            }.onFailure { cause ->
                publishResponse(cmd, FailedCmdResponse(cause.message, cmd.cmdId))
            }
    }

    private fun handleStopConsumersCmd(cmd: StopConsumersBalancingCmd) {
        stopConsumersCmdHandler.handle(cmd.consumerCount)
            .onSuccess { cmdResult ->
                publishResponse(
                    cmd,
                    StopConsumersBalancingCmdResponse(cmdResult.stoppedShardIds, cmdResult.activeConsumers, cmd.cmdId)
                )
            }.onFailure { cause ->
                publishResponse(cmd, FailedCmdResponse(cause.message, cmd.cmdId))
            }
    }

    private fun handleStartConsumersCmdResponse(response: BalancingCmdResponse) {
        val promise = pendingStartCmdResponses.remove(response.cmdId)
        val completed = when (response) {
            is FailedCmdResponse -> promise?.tryFail(BalancingException(response.cause)) == true
            is StartConsumersBalancingCmdResponse -> promise?.tryComplete(response.activeConsumers) == true
            else -> throw BalancingException("Unexpected start command response type ${response::class.java.name}")
        }
        if (!completed) {
            logger.warn { "Unable to complete response of command ${response.cmdId}" }
        }
    }

    private fun handleStopConsumersCmdResponse(response: BalancingCmdResponse) {
        val promise = pendingStopCmdResponses.remove(response.cmdId)
        val completed = when (response) {
            is FailedCmdResponse -> promise?.tryFail(BalancingException(response.cause)) == true
            is StopConsumersBalancingCmdResponse -> promise?.tryComplete(
                StopConsumersCmdResult(
                    response.stoppedShards,
                    response.activeConsumers
                )
            ) == true
            else -> throw BalancingException("Unexpected stop command response type ${response::class.java.name}")
        }
        if (!completed) {
            logger.warn { "Unable to complete response of command ${response.cmdId}" }
        }
    }


    override fun sendStopConsumersCmd(
        destinationNodeId: OrchestraClusterNodeId,
        consumerCount: Int
    ): Future<StopConsumersCmdResult> {
        val promise = Promise.promise<StopConsumersCmdResult>()

        val destinationChannel = stopConsumersCmdChannelNameOf(destinationNodeId)
        val cmdId = newCmdId()
        val cmd = StopConsumersBalancingCmd(consumerCount, cmdId, stopConsumersCmdResponseChannelName)

        pendingStopCmdResponses[cmdId] = promise

        publishCmd(destinationChannel, cmd)
        val future = promise.future()
        handleCmdDurationExceed(
            cmdId,
            promise,
            future,
            destinationChannel,
            pendingStopCmdResponses as MutableMap<CmdId, Promise<*>>
        )
        return future
    }


    override fun sendStartConsumersCmd(
        destinationNodeId: OrchestraClusterNodeId,
        shardIds: List<ShardId>
    ): Future<Int> {
        val promise = Promise.promise<Int>()

        val destinationChannel = startConsumersCmdChannelNameOf(destinationNodeId)
        val cmdId = newCmdId()
        val cmd = StartConsumersBalancingCmd(shardIds, cmdId, startConsumersCmdResponseChannelName)

        pendingStartCmdResponses[cmdId] = promise

        publishCmd(destinationChannel, cmd)
        val future = promise.future()
        handleCmdDurationExceed(
            cmdId,
            promise,
            future,
            destinationChannel,
            pendingStartCmdResponses as MutableMap<CmdId, Promise<*>>
        )
        return future
    }

    private fun publishResponse(cmd: BalancingCmd, response: BalancingCmdResponse) {
        sender.send(Request.cmd(Command.PUBLISH).arg(cmd.responseChannel).arg(JsonObject.mapFrom(response).encode()))
    }

    private fun publishCmd(destinationChannel: String, cmd: BalancingCmd) {
        sender.send(Request.cmd(Command.PUBLISH).arg(destinationChannel).arg(JsonObject.mapFrom(cmd).encode()))
    }

    private fun newCmdId(): CmdId = UUID.randomUUID()

    private fun handleCmdDurationExceed(
        cmdId: CmdId,
        promise: Promise<*>,
        future: Future<*>,
        destinationChannelName: String,
        pendingResponseMap: MutableMap<CmdId, Promise<*>>
    ) {
        vertx.setTimer(balancingCommandDurationThresholdMillis) {
            if (!future.isComplete) {
                promise.tryFail(BalancingException("Processing of command on destination $destinationChannelName did takes longer as allowed <$balancingCommandDurationThresholdMillis"))
                pendingResponseMap.remove(cmdId)
            }
        }
    }
}

typealias CmdId = UUID

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = StartConsumersBalancingCmd::class, name = "startCmd"),
    JsonSubTypes.Type(value = StopConsumersBalancingCmd::class, name = "stopCmd")
)
private abstract class BalancingCmd(
    @field:JsonProperty("cmdId") val cmdId: CmdId,
    @field:JsonProperty("responseChannel") val responseChannel: String
)

private class StartConsumersBalancingCmd(
    @field:JsonProperty("shardsToStart") val shardsToStart: List<ShardId>,
    cmdId: CmdId,
    responseChannel: String
) : BalancingCmd(cmdId, responseChannel)

private class StopConsumersBalancingCmd(
    val consumerCount: Int,
    cmdId: CmdId,
    responseChannel: String
) : BalancingCmd(cmdId, responseChannel)


@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = StartConsumersBalancingCmdResponse::class, name = "startCmdResponse"),
    JsonSubTypes.Type(value = StopConsumersBalancingCmdResponse::class, name = "stopCmdResponse"),
    JsonSubTypes.Type(value = FailedCmdResponse::class, name = "failedCmdResponse")
)
private abstract class BalancingCmdResponse(@field:JsonProperty("cmdId") val cmdId: CmdId)
private class FailedCmdResponse(@field:JsonProperty("cause") val cause: String?, cmdId: CmdId) :
    BalancingCmdResponse(cmdId)

private class StartConsumersBalancingCmdResponse(
    @field:JsonProperty("activeConsumers") val activeConsumers: Int,
    cmdId: CmdId
) : BalancingCmdResponse(cmdId)

private class StopConsumersBalancingCmdResponse(
    @field:JsonProperty("stoppedShards") val stoppedShards: List<ShardId>,
    @field:JsonProperty("activeConsumers") val activeConsumers: Int,
    cmdId: CmdId,
) : BalancingCmdResponse(cmdId)


fun interface StopConsumersCmdHandler {
    fun handle(consumerCount: Int): Future<StopConsumersCmdResult>
}

fun interface StartConsumersCmdHandler {
    fun handle(shardIds: List<ShardId>): Future<Int>
}