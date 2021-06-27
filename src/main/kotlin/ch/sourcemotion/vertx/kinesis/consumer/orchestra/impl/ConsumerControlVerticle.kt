package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumerCmd.FailureCodes.CONSUMER_START_FAILURE
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StopConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KLogging
import kotlin.LazyThreadSafetyMode.NONE

internal class ConsumerControlVerticle : CoroutineVerticle() {

    private companion object : KLogging()

    private val consumerDeploymentIds = HashMap<ShardId, String>()

    private val options by lazy(NONE) { config.mapTo(Options::class.java) }

    override suspend fun start() {
        vertx.eventBus().localConsumer(EventBusAddr.consumerControl.stopConsumerCmd, ::onStopConsumerCmd)
            .completionHandlerAwait()
        vertx.eventBus().localConsumer(EventBusAddr.consumerControl.startConsumerCmd, ::onStartConsumerCmd)
            .completionHandlerAwait()

        notifyAboutActiveConsumers() // Directly after start there would be no consumer, so we start the detection.
        logger.debug { "Consumer control started with options \"$options\"" }
        logger.info {
            "VKCO instance configure to consume max. ${options.loadConfiguration.maxShardsCount} " +
                    "shards from stream ${options.clusterName.streamName}"
        }
    }

    private fun onStopConsumerCmd(msg: Message<StopConsumerCmd>) {
        val shardId = msg.body().shardId
        val deploymentIdToUndeploy = consumerDeploymentIds.remove(shardId)
        if (deploymentIdToUndeploy.isNotNullOrBlank()) {
            vertx.undeploy(deploymentIdToUndeploy) {
                logAndNotifyAboutActiveConsumers()
                if (it.succeeded()) {
                    logger.info { "Consumer for shard \"$shardId\" stopped" }
                    msg.ack()
                } else {
                    msg.fail(StopConsumerCmd.CONSUMER_STOP_FAILURE, it.cause().message)
                }
            }
        } else {
            logger.info { "Unable to stop consumer for shard \"$shardId\", because no known consumer for this shard" }
            logAndNotifyAboutActiveConsumers()
            msg.fail(StopConsumerCmd.UNKNOWN_CONSUMER_FAILURE, "Unknown consumer")
        }
    }

    private fun onStartConsumerCmd(msg: Message<StartConsumerCmd>) {
        val cmd = msg.body()
        val shardId = cmd.shardId
        if (consumerDeploymentIds.containsKey(shardId)) {
            msg.ack()
            return
        }
        if (consumerCapacity() <= 0) {
            logger.warn { "Consumer control unable to start consumer for shard $shardId because no consumer capacity left" }
            logAndNotifyAboutActiveConsumers()
            msg.fail(StartConsumerCmd.CONSUMER_CAPACITY_FAILURE, "No consumer capacity left")
            return
        }
        launch {
            try {
                withTimeout(options.consumerDeploymentTimeoutMillis) {
                    val consumerOptions = createConsumerVerticleOptions(shardId, cmd.iteratorStrategy)
                    startConsumer(
                        options.consumerVerticleClass,
                        JsonObject(options.consumerVerticleConfig),
                        consumerOptions
                    )
                }
                logAndNotifyAboutActiveConsumers()
                msg.ack()
            } catch (e: Exception) {
                logger.warn(e) { "Consumer control unable to start consumer for shard \"$shardId\"" }
                logAndNotifyAboutActiveConsumers()
                msg.fail(CONSUMER_START_FAILURE, e.message)
            }
        }
    }

    private suspend fun startConsumer(
        consumerVerticleClass: String,
        consumerVerticleConfig: JsonObject,
        consumerOptions: KinesisConsumerVerticleOptions
    ) {
        vertx.runCatching {
            deployVerticleAwait(
                consumerVerticleClass,
                deploymentOptionsOf(config = JsonObject.mapFrom(consumerOptions).mergeIn(consumerVerticleConfig, true))
            )
        }.onSuccess { deploymentId ->
            consumerDeploymentIds[consumerOptions.shardId] = deploymentId
            logger.info { "Consumer started for stream ${options.clusterName.streamName} / shard \"${consumerOptions.shardId}\"." }
        }.onFailure {
            logger.error(it) { "Failed to start consumer of stream ${options.clusterName.streamName} / shard ${consumerOptions.shardId}" }
        }
    }

    private fun logAndNotifyAboutActiveConsumers() {
        if (consumerDeploymentIds.keys.isEmpty()) {
            logger.info {
                "Currently no shards get consumed on stream \"${options.clusterName.streamName}\". " +
                        "This VKCO instance has to capacity to consume ${consumerCapacity()} shards"
            }
        } else {
            logger.info {
                "Currently the shards ${consumerDeploymentIds.keys.joinToString()} " +
                        "are consumed on stream \"${options.clusterName.streamName}\". " +
                        "This VKCO instance has to capacity to consume ${consumerCapacity()} further shards"
            }
        }
        notifyAboutActiveConsumers()
    }

    private fun notifyAboutActiveConsumers() {
        vertx.eventBus().send(EventBusAddr.detection.consumedShardCountNotification, consumerDeploymentIds.size)
    }

    private fun consumerCapacity() = options.loadConfiguration.maxShardsCount - consumerDeploymentIds.size

    private fun createConsumerVerticleOptions(
        shardId: ShardId,
        shardIteratorStrategy: ShardIteratorStrategy
    ) =
        KinesisConsumerVerticleOptions(
            shardId,
            options.clusterName,
            shardIteratorStrategy,
            options.errorHandling,
            options.sequenceNumberImportAddress,
            options.shardProgressExpirationMillis,
            options.fetcherOptions
        )

    @JsonIgnoreProperties(ignoreUnknown = true)
    internal class Options(
        val clusterName: OrchestraClusterName,
        val redisHeimdallOptions: RedisHeimdallOptions,
        val fetcherOptions: FetcherOptions,
        val shardIteratorStrategy: ShardIteratorStrategy,
        val loadConfiguration: LoadConfiguration,
        val errorHandling: ErrorHandling,
        val consumerDeploymentTimeoutMillis: Long,
        val consumerVerticleClass: String,
        val consumerVerticleConfig: Map<String, Any>,
        val sequenceNumberImportAddress: String? = null,
        val shardProgressExpirationMillis: Long
    )
}


internal fun VertxKinesisOrchestraOptions.asConsumerControlOptions() = ConsumerControlVerticle.Options(
    OrchestraClusterName(applicationName, streamName),
    redisOptions,
    fetcherOptions,
    shardIteratorStrategy,
    loadConfiguration,
    errorHandling,
    consumerDeploymentTimeout.toMillis(),
    consumerVerticleClass,
    consumerVerticleOptions.map,
    kclV1ImportOptions?.importAddress,
    shardProgressExpiration.toMillis()
)
