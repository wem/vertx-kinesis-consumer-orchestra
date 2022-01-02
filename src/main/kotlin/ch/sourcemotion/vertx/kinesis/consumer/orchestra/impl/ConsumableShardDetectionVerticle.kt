package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumableShardDetectionService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

/**
 * Verticle that will detect consumable shards. The detection will stopp if the VKCO instance consumes the configured max.
 * of shards. And it's able to restart detecting as well if consumer(s) was stopped.
 */
internal class ConsumableShardDetectionVerticle : CoroutineVerticle(), ConsumableShardDetectionService {

    private companion object : KLogging()

    private lateinit var options: Options
    private lateinit var kinesisClient: KinesisAsyncClient
    private lateinit var shardStatePersistence : ShardStatePersistenceServiceAsync

    override suspend fun start() {
        options = config.mapTo(Options::class.java)
        kinesisClient =
            SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
                .createKinesisAsyncClient(vertx.orCreateContext)
        shardStatePersistence = ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
        ConsumableShardDetectionService.exposeService(vertx, this)
    }

    override fun getConsumableShards(): Future<List<ShardId>> {
        val p = Promise.promise<List<ShardId>>()
        launch {
            val existingShards = kinesisClient.listShardsSafe(options.clusterName.streamName)
            val existingShardIds = existingShards.map { it.shardIdTyped() }
            val finishedShardIdsAsync = async { shardStatePersistence.getFinishedShardIds(existingShardIds) }
            val shardIdsInProgressAsync = async { shardStatePersistence.getShardIdsInProgress(existingShardIds) }
            val finishedShardIds = finishedShardIdsAsync.await()
            val shardIdsInProgress = shardIdsInProgressAsync.await()
            val unavailableShardIds = shardIdsInProgress + finishedShardIds

            val availableShards = existingShards
                .filterNot { shard -> unavailableShardIds.contains(shard.shardIdTyped()) }

            val consumableShardIds = ConsumableShardIdListFactory.create(
                existingShards,
                availableShards,
                finishedShardIds
            )
            p.complete(consumableShardIds)
        }
        return p.future()
    }

    data class Options(
        val clusterName: OrchestraClusterName
    )
}
