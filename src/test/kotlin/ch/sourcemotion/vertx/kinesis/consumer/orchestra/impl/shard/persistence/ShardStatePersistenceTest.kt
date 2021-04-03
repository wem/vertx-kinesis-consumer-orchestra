package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asSequenceNumberAt
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.undeployAwait
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.LazyThreadSafetyMode.NONE

internal class ShardStatePersistenceTest : AbstractRedisTest(false) {

    companion object {
        private const val APPLICATION_NAME = "test-application"
        private const val STREAM_NAME = "test-stream"

        private const val DEFAULT_TEST_EXPIRATION_MILLIS = 1000L

        private val shardId = ShardIdGenerator.generateShardId()
    }

    private val sut by lazy(NONE) { ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx) }

    private var deploymentId: String? = null

    @BeforeEach
    internal fun deployShardStatePersistenceService() = asyncBeforeOrAfter {
        deployShardStatePersistenceService(DEFAULT_TEST_EXPIRATION_MILLIS)

        val options = RedisShardStatePersistenceServiceVerticleOptions(
            APPLICATION_NAME,
            STREAM_NAME,
            redisHeimdallOptions.apply { redisOptions.maxPoolSize = 8 },
            DEFAULT_TEST_EXPIRATION_MILLIS
        )
        deploymentId = vertx.deployVerticleAwait(
            RedisShardStatePersistenceServiceVerticle::class.java.name, DeploymentOptions().setConfig(
                JsonObject.mapFrom(options)
            )
        )
    }

    @AfterEach
    internal fun undeployShardPersistenceVerticle() = asyncBeforeOrAfter {
        deploymentId?.let { vertx.undeployAwait(it) }
    }

    @Test
    internal fun flagShardInProgress(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress().shouldContainExactly(shardId)
    }

    @Test
    internal fun swap_shard_progress_flag(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress().shouldContainExactly(shardId)
        sut.flagShardNoMoreInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress().shouldBeEmpty()
    }

    @Test
    internal fun shard_progress_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.getShardIdsInProgress().shouldContainExactly(shardId)
        delay(DEFAULT_TEST_EXPIRATION_MILLIS * 2)
        sut.getShardIdsInProgress().shouldBeEmpty()
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

        sut.getFinishedShardIds().shouldContainExactlyInAnyOrder(shardIds)
    }

    @Test
    internal fun save_finished_shard_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardIds = ShardIdGenerator.generateShardIdList(10)
        shardIds.forEach { sut.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION_MILLIS) }
        sut.getFinishedShardIds().shouldContainExactlyInAnyOrder(shardIds)
        delay(DEFAULT_TEST_EXPIRATION_MILLIS * 2)
        sut.getFinishedShardIds().shouldBeEmpty()
    }


    @Test
    internal fun flag_merge_parents_ready_to_reshard(testContext: VertxTestContext) = asyncTest(testContext) {
        val parentShardId = ShardIdGenerator.generateShardId()
        val adjacentParentShardId = ShardIdGenerator.generateShardId(1)
        val childShardId = ShardIdGenerator.generateShardId(2)

        sut.flagMergeParentReadyToReshard(parentShardId, childShardId).shouldBeFalse()
        sut.flagMergeParentReadyToReshard(parentShardId, childShardId).shouldBeFalse()
        sut.flagMergeParentReadyToReshard(adjacentParentShardId, childShardId).shouldBeTrue()
        sut.flagMergeParentReadyToReshard(adjacentParentShardId, childShardId).shouldBeTrue()
        sut.deleteMergeParentsReshardingReadyFlag(childShardId)
        sut.flagMergeParentReadyToReshard(adjacentParentShardId, childShardId).shouldBeFalse()
    }

    /**
     * Test for a lot of concurrent commands
     */
    @Test
    internal fun concurrent_merge_resharding_event_count(testContext: VertxTestContext) =
        asyncTest(testContext, 100) { checkpoint ->
            val coroutineCount = 100
            var shardIdCounter = 0
            repeat(coroutineCount) {
                defaultTestScope.launch {
                    val parentShardId = ShardIdGenerator.generateShardId(shardIdCounter++)
                    val adjacentParentShardId = ShardIdGenerator.generateShardId(shardIdCounter++)
                    val childShardId = ShardIdGenerator.generateShardId(shardIdCounter++)
                    sut.flagMergeParentReadyToReshard(parentShardId, childShardId).shouldBeFalse()
                    sut.flagMergeParentReadyToReshard(adjacentParentShardId, childShardId).shouldBeTrue()
                    checkpoint.flag()
                }
            }
        }

    @Test
    internal fun get_shardids_in_progress_no_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) { sut.getShardIdsInProgress().shouldBeEmpty() }

    @Test
    internal fun get_shardids_in_progress_one_shard_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            sut.flagShardInProgress(shardId).shouldBeTrue()
            sut.getShardIdsInProgress().shouldContainExactly(shardId)
        }

    @Test
    internal fun get_shardids_in_progress_many_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardIdList = MutableList(100) {
                val shardNumber = it + 1
                ShardIdGenerator.generateShardId(shardNumber)
            }.apply { add(shardId) }
            shardIdList.forEach { shardId ->
                sut.flagShardInProgress(shardId).shouldBeTrue()
            }

            sut.getShardIdsInProgress().shouldContainExactlyInAnyOrder(shardIdList)
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

    @Test
    internal fun concurrent_merge_resharding_event_count_closed_connection(testContext: VertxTestContext) =
        asyncTest(testContext, 100) { checkpoint ->
            var shardIdCounter = 0
            val jobCount = 100
            repeat(jobCount) { jobNumber ->
                defaultTestScope.launch {
                    val parentShardId = ShardIdGenerator.generateShardId(shardIdCounter++)
                    val adjacentParentShardId = ShardIdGenerator.generateShardId(shardIdCounter++)
                    val childShardId = ShardIdGenerator.generateShardId(shardIdCounter++)
                    sut.flagMergeParentReadyToReshard(parentShardId, childShardId).shouldBeFalse()
                    sut.flagMergeParentReadyToReshard(adjacentParentShardId, childShardId).shouldBeTrue()
                    checkpoint.flag()
                }

                if (jobNumber == jobCount / 2) {
                    closeConnectionToRedis()
                }
            }

            removeRedisToxiesAfter(redisHeimdallOptions.reconnectInterval * 3)
        }
}
