package ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import io.vertx.codegen.annotations.ProxyGen
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.awaitResult

/**
 * This is the entry point to customize how the shard iterators / consumers states are persisted
 *
 * The [ShardStatePersistenceService] is responsible to:
 * - Persist the shard iterator of each consumer.
 * - The state of each consumer is on duty, or which shards are currently consumed.
 * - Persistence of the Kinesis shard iterator of each consumed shard.
 * - The count of done merge parent shard consumers of the resulting child shard.
 * - Keep alive of shard progress to avoid a shard get consumed by more than one consumer at once.
 *
 * To expose your own implementation please use [ShardStatePersistenceServiceFactory.expose]
 */
@ProxyGen
interface ShardStatePersistenceService {

    /**
     * @return List of shard ids they are currently in progress. Means they are fetched by a consumer of an orchestra instance.
     */
    fun getShardIdsInProgress(handler: Handler<AsyncResult<List<String>>>)

    /**
     * Flags a shard as currently fetched by consumer of an orchestra instance and so in progress. The flag should
     * expire according to [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.shardProgressExpiration].
     * The expiration should prevent the orchestration from death locks if an orchestra instance was not gracefully
     * stopped.
     */
    fun flagShardInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>)

    /**
     * Removes a the flag that's a shard is currently fetched by a consumer of an orchestra instance
     */
    fun flagShardNoMoreInProgress(shardId: String, handler: Handler<AsyncResult<Boolean>>)

    /**
     * Like [flagShardInProgress], but will keep the flagged state until [flagShardNoMoreInProgress] is called.
     * Different to [flagShardInProgress] this call should hold the progress flag keep alive at least
     * as [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.shardProgressExpiration]
     * is configured.
     */
    fun startShardProgressAndKeepAlive(shardId: String, handler: Handler<AsyncResult<Void?>>)

    /**
     * Persistence of the current "cursor" or in context of Kinesis sequence number of the latest successful proceeded
     * record.
     */
    fun saveConsumerShardSequenceNumber(
        shardId: String,
        sequenceNumber: String,
        iteratorPosition: SequenceNumberIteratorPosition,
        handler: Handler<AsyncResult<Void?>>
    )

    /**
     * Should call [handler] with the most recent value of the [saveConsumerShardSequenceNumber] call.
     * The [JsonObject] parameter of [handler] represents a [SequenceNumber] instance.
     */
    fun getConsumerShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<JsonObject?>>)

    /**
     * Deletion of any value of previous [saveConsumerShardSequenceNumber] calls. [getConsumerShardSequenceNumber] should call
     * its handler with null.
     */
    fun deleteShardSequenceNumber(shardId: String, handler: Handler<AsyncResult<Boolean>>)

    /**
     * Saves a finished flag for the given shard id.
     */
    fun saveFinishedShard(shardId: String, expirationMillis: Long, handler: Handler<AsyncResult<Void?>>)

    /**
     * Calls [handler] with a list of finished shard ids. By previous calls on [saveFinishedShard])
     */
    fun getFinishedShardIds(handler: Handler<AsyncResult<List<String>>>)

    /**
     * Should increment (and call [handler] with the incrementation result) a counter related to (child) shard id.
     * This counter signals when a merge resharding event can get published within a whole consumer orchestra.
     */
    fun incrAndGetMergeReshardingEventCount(childShardId: String, handler: Handler<AsyncResult<Int>>)

    /**
     * Deletes the incrementation state of [incrAndGetMergeReshardingEventCount].
     */
    fun deleteMergeReshardingEventCount(childShardId: String, handler: Handler<AsyncResult<Void?>>)
}

class ShardStatePersistenceServiceAsync(private val delegate: ShardStatePersistenceService) :
    ShardStatePersistenceService by delegate {
    suspend fun getShardIdsInProgress(): ShardIdList =
        awaitResult<List<String>> { getShardIdsInProgress(it) }.map { it.asShardIdTyped() }

    suspend fun flagShardInProgress(shardId: ShardId) = awaitResult<Boolean> { flagShardInProgress(shardId.id, it) }

    suspend fun flagShardNoMoreInProgress(shardId: ShardId) =
        awaitResult<Boolean> { flagShardNoMoreInProgress(shardId.id, it) }

    suspend fun startShardProgressAndKeepAlive(shardId: ShardId) =
        awaitResult<Void?> { startShardProgressAndKeepAlive(shardId.id, it) }

    suspend fun saveConsumerShardSequenceNumber(shardId: ShardId, sequenceNumber: SequenceNumber) =
        awaitResult<Void?> {
            saveConsumerShardSequenceNumber(
                shardId.id,
                sequenceNumber.number,
                sequenceNumber.iteratorPosition,
                it
            )
        }

    suspend fun getConsumerShardSequenceNumber(shardId: ShardId): SequenceNumber? =
        awaitResult<JsonObject?> { getConsumerShardSequenceNumber(shardId.id, it) }?.mapTo(SequenceNumber::class.java)

    suspend fun deleteShardSequenceNumber(shardId: ShardId) =
        awaitResult<Boolean> { deleteShardSequenceNumber(shardId.id, it) }

    suspend fun saveFinishedShard(shardId: ShardId, expirationMillis: Long) =
        awaitResult<Void?> { saveFinishedShard(shardId.id, expirationMillis, it) }

    suspend fun getFinishedShardIds(): ShardIdList =
        awaitResult<List<String>> { getFinishedShardIds(it) }.map { it.asShardIdTyped() }

    suspend fun getMergeReshardingEventCount(childShardId: ShardId) =
        awaitResult<Int> { incrAndGetMergeReshardingEventCount(childShardId.id, it) }

    suspend fun deleteMergeReshardingEventCount(childShardId: ShardId) =
        awaitResult<Void?> { deleteMergeReshardingEventCount(childShardId.id, it) }
}
