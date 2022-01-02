package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumerControlService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StopConsumersCmdResult
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import mu.KLogging

internal class ConsumerControlVerticle : CoroutineVerticle(), ConsumerControlService {

    private companion object : KLogging()

    private val deployedConsumers = ArrayList<DeployedConsumer>()
    private lateinit var options: Options
    private lateinit var shardStatePersistenceService: ShardStatePersistenceServiceAsync

    override suspend fun start() {
        options = config.mapTo(Options::class.java)
        shardStatePersistenceService =
            ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
        ConsumerControlService.exposeService(vertx, this)
    }

    override fun stopConsumer(shardId: ShardId): Future<Void> {
        val consumerToStop = deployedConsumers.firstOrNull { it.shardId == shardId }
        return if (consumerToStop != null) {
            vertx.undeploy(consumerToStop.deploymentId)
                .onSuccess { deployedConsumers.remove(consumerToStop) }
                .onFailure { cause -> logger.warn(cause) { "Failed to undeploy consumer of shard ${consumerToStop.shardId}" } }
                .compose { Future.succeededFuture() }
        } else Future.succeededFuture()
    }

    override fun stopConsumers(consumerCount: Int): Future<StopConsumersCmdResult> {
        val consumersToStop = deployedConsumers.take(consumerCount)
        val p = Promise.promise<StopConsumersCmdResult>()

        launch {
            coroutineScope {
                consumersToStop.forEach { consumerToStop ->
                    try {
                        vertx.undeploy(consumerToStop.deploymentId).await()
                    } catch (e: Exception) {
                        logger.warn(e) { "Failed to undeploy consumer of shard ${consumerToStop.shardId}" }
                    }
                    deployedConsumers.remove(consumerToStop)
                }
            }
            p.complete(StopConsumersCmdResult(consumersToStop.map { it.shardId }, deployedConsumers.size))
        }

        return p.future()
    }

    override fun startConsumers(shardIds: List<ShardId>): Future<Int> {
        val deployConsumerShardIds = deployedConsumers.map { deployedConsumer -> deployedConsumer.shardId }
        val shardIdsToStartConsumer = shardIds.filterNot { deployConsumerShardIds.contains(it) }
        val p = Promise.promise<Int>()
        launch {
            shardIdsToStartConsumer.forEach { shardIdToStartConsumer ->
                try {
                    val shardIteratorStrategy = shardIteratorStrategy(shardIdToStartConsumer)
                    val consumerVerticleOptions =
                        createConsumerVerticleOptions(shardIdToStartConsumer, shardIteratorStrategy)
                    val customVerticleOption = JsonObject(options.consumerVerticleConfig)
                    val verticleOptions =
                        JsonObject.mapFrom(consumerVerticleOptions).mergeIn(customVerticleOption, true)
                    withTimeout(options.consumerDeploymentTimeoutMillis) {
                        val deploymentId = vertx.deployVerticle(
                            options.consumerVerticleClass,
                            deploymentOptionsOf(config = verticleOptions)
                        ).await()
                        deployedConsumers.add(DeployedConsumer(shardIdToStartConsumer, deploymentId))
                    }
                } catch (e: Exception) {
                    logger.warn(e) { "Failed to deploy consumer of shard $shardIdToStartConsumer" }
                }
            }
            p.complete(deployedConsumers.size)
        }
        return p.future()
    }

    private suspend fun shardIteratorStrategy(shardId: ShardId): ShardIteratorStrategy {
        return if (shardStatePersistenceService.getConsumerShardSequenceNumber(shardId) == null) {
            ShardIteratorStrategy.FORCE_LATEST
        } else options.shardIteratorStrategy
    }

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
    internal data class Options(
        val clusterName: OrchestraClusterName,
        val redisHeimdallOptions: RedisHeimdallOptions,
        val fetcherOptions: FetcherOptions,
        val shardIteratorStrategy: ShardIteratorStrategy,
        val errorHandling: ErrorHandling,
        val consumerDeploymentTimeoutMillis: Long,
        val consumerVerticleClass: String,
        val consumerVerticleConfig: Map<String, Any>,
        val sequenceNumberImportAddress: String? = null,
        val shardProgressExpirationMillis: Long
    )
}

private data class DeployedConsumer(val shardId: ShardId, val deploymentId: String)


internal fun VertxKinesisOrchestraOptions.asConsumerControlOptions() = ConsumerControlVerticle.Options(
    OrchestraClusterName(applicationName, streamName),
    redisOptions,
    fetcherOptions,
    shardIteratorStrategy,
    errorHandling,
    consumerDeploymentTimeout.toMillis(),
    consumerVerticleClass,
    consumerVerticleOptions.map,
    kclV1ImportOptions?.importAddress,
    shardProgressExpiration.toMillis()
)