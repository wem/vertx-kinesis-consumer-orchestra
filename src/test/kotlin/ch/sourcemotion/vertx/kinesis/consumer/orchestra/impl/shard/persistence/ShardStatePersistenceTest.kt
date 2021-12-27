package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asSequenceNumberAt
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.undeployAwait
import kotlinx.coroutines.delay
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.LazyThreadSafetyMode.NONE

internal class ShardStatePersistenceTest : AbstractRedisTest(false) {

    companion object {
        private const val DEFAULT_TEST_EXPIRATION_MILLIS = 1000L
        private val shardId = ShardIdGenerator.generateShardId()
    }

    private val sut by lazy(NONE) { ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx) }

    private var deploymentId: String? = null

    @BeforeEach
    internal fun deployShardStatePersistenceService() = asyncBeforeOrAfter {
        deployRedisShardStatePersistenceServiceVerticle(DEFAULT_TEST_EXPIRATION_MILLIS)
    }

    @AfterEach
    internal fun undeployShardPersistenceVerticle() = asyncBeforeOrAfter {
        deploymentId?.let { vertx.undeployAwait(it) }
    }

    @Test
    internal fun flag_shard_in_progress(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress(listOf(shardId)).shouldContainExactly(shardId)
    }

    @Test
    internal fun flag_shards_in_progress(testContext: VertxTestContext) = asyncTest(testContext) {
        val shards = ShardIdGenerator.generateShardIdList(10)
        sut.flagShardsInProgress(shards, 10000)
        val shardsInProgress = sut.getShardIdsInProgress(shards)
        shardsInProgress.shouldContainExactlyInAnyOrder(shards)
    }

    @Test
    internal fun flag_shards_in_progress_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        val shards = ShardIdGenerator.generateShardIdList(10)
        sut.flagShardsInProgress(shards, 100)
        delay(101)
        val shardsInProgress = sut.getShardIdsInProgress(shards)
        shardsInProgress.shouldBeEmpty()
    }

    @Test
    internal fun swap_shard_progress_flag(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress(listOf(shardId)).shouldContainExactly(shardId)
        sut.flagShardNoMoreInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress(listOf(shardId)).shouldBeEmpty()
    }

    @Test
    internal fun shard_progress_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress(listOf(shardId)).shouldContainExactly(shardId)
        delay(DEFAULT_TEST_EXPIRATION_MILLIS * 2)
        sut.getShardIdsInProgress(listOf(shardId)).shouldBeEmpty()
    }

    @Test
    internal fun save_consumer_shard_sequence(testContext: VertxTestContext) = asyncTest(testContext) {
        val sequenceNumber = "sequencenumber".asSequenceNumberAt()
        sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
        sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
    }

    @Test
    internal fun delete_shard_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        val sequenceNumber = "sequencenumber".asSequenceNumberAt()
        sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
        sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        sut.deleteShardSequenceNumber(shardId).shouldBeTrue()
        sut.getConsumerShardSequenceNumber(shardId).shouldBeNull()
    }

    @Test
    internal fun save_finished_shard(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardIds = ShardIdGenerator.generateShardIdList(2)
        shardIds.forEach { sut.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION_MILLIS) }

        sut.getFinishedShardIds(shardIds).shouldContainExactlyInAnyOrder(shardIds)
    }

    @Test
    internal fun save_many_finished_shards(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardCount = 10000
        val shardIds = ShardIdGenerator.generateShardIdList(shardCount)
        shardIds.forEach { sut.saveFinishedShard(it, 30000) } // For 10000 entries it can take a longer time

        val finishedShardIds = sut.getFinishedShardIds(shardIds)
        finishedShardIds.shouldHaveSize(shardCount)
        finishedShardIds.shouldContainExactlyInAnyOrder(shardIds)
    }

    @Test
    internal fun save_finished_shard_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardIds = ShardIdGenerator.generateShardIdList(10)
        shardIds.forEach { sut.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION_MILLIS) }
        sut.getFinishedShardIds(shardIds).shouldContainExactlyInAnyOrder(shardIds)
        delay(DEFAULT_TEST_EXPIRATION_MILLIS * 2)
        sut.getFinishedShardIds(shardIds).shouldBeEmpty()
    }

    @Test
    internal fun get_shardids_in_progress_no_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) { sut.getShardIdsInProgress(listOf(shardId)).shouldBeEmpty() }

    @Test
    internal fun get_shardids_in_progress_one_shard_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            sut.flagShardInProgress(shardId).shouldBeTrue()
            sut.getShardIdsInProgress(listOf(shardId)).shouldContainExactly(shardId)
        }

    @Test
    internal fun get_shard_ids_in_progress_many_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardIds = IntRange(0, 99).map { ShardIdGenerator.generateShardId(it) }
            shardIds.forEach { shardId ->
                sut.flagShardInProgress(shardId).shouldBeTrue()
            }
            sut.getShardIdsInProgress(shardIds).shouldContainExactlyInAnyOrder(shardIds)
        }

    /*
     * Network issue tests
     */

    @Test
    internal fun save_consumer_shard_sequence_upstream_issue(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val sequenceNumber = "sequencenumber".asSequenceNumberAt()

            preventDataToRedisPassingAfter(2)

            removeRedisToxiesAfter(redisHeimdallOptions.reconnectInterval * 2)

            sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
            sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        }

    @Test
    internal fun save_consumer_shard_sequence_downstream_issue(testContext: VertxTestContext) =
        asyncTest(testContext) {

            val sequenceNumber = "sequencenumber".asSequenceNumberAt()

            preventDataFromRedisPassingAfter(2)

            removeRedisToxiesAfter(redisHeimdallOptions.reconnectInterval * 2)

            sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
            sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        }

    @Test
    internal fun save_consumer_shard_sequence_closed_connection(testContext: VertxTestContext) =
        asyncTest(testContext) {

            val sequenceNumber = "sequencenumber".asSequenceNumberAt()

            closeConnectionToRedis()

            removeRedisToxiesAfter(redisHeimdallOptions.reconnectInterval * 3)

            sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
            sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        }
}
