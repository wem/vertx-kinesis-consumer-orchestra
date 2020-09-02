package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asSequenceNumberAt
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.ints.shouldBeBetween
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.core.DeploymentOptions
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.coroutines.resume

internal class ShardStatePersistenceTest : AbstractRedisTest() {

    companion object {
        private const val APPLICATION_NAME = "test-application"
        private const val STREAM_NAME = "test-stream"

        private const val DEFAULT_TEST_EXPIRATION_MILLIS = 1000L

        private val shardId = ShardIdGenerator.generateShardId()
    }

    @BeforeEach
    internal fun deployShardStatePersistenceService(testContext: VertxTestContext) = asyncTest(testContext) {
        val options = RedisShardStatePersistenceServiceVerticleOptions(
            APPLICATION_NAME,
            STREAM_NAME,
            redisOptions,
            DEFAULT_TEST_EXPIRATION_MILLIS
        )
        vertx.deployVerticleAwait(
            RedisShardStatePersistenceServiceVerticle::class.java.name, DeploymentOptions().setConfig(
                JsonObject.mapFrom(options)
            )
        )
    }

    private val sut by lazy { ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx) }

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
    internal fun start_shard_progress_keepalive(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.startShardProgressAndKeepAlive(shardId)
        suspendCancellableCoroutine<Unit> { cont ->
            vertx.setTimer(DEFAULT_TEST_EXPIRATION_MILLIS * 3) {
                cont.resume(Unit)
            }
        }
        sut.getShardIdsInProgress().shouldContainExactly(shardId)
        sut.flagShardNoMoreInProgress(shardId).shouldBeTrue()
        delay(DEFAULT_TEST_EXPIRATION_MILLIS * 3)
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
        val foundShardIds = sut.getFinishedShardIds()
        foundShardIds.shouldContainExactlyInAnyOrder(shardIds)
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
    internal fun merge_resharding_event_count(testContext: VertxTestContext) = asyncTest(testContext) {
        val childShardId = ShardIdGenerator.generateShardId()
        sut.getMergeReshardingEventCount(childShardId).shouldBe(1)
        sut.getMergeReshardingEventCount(childShardId).shouldBe(2)
        sut.deleteMergeReshardingEventCount(childShardId)
        sut.getMergeReshardingEventCount(childShardId).shouldBe(1)
    }

    /**
     * Test for a lot of concurrent commands
     */
    @Test
    internal fun concurrent_merge_resharding_event_count(testContext: VertxTestContext) =
        asyncTest(testContext, 100) { checkpoint ->
            val childShardId = ShardIdGenerator.generateShardId()
            val coroutineCount = 100
            repeat(coroutineCount) {
                defaultTestScope.launch {
                    sut.getMergeReshardingEventCount(childShardId).shouldBeBetween(1, coroutineCount)
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

            vertx.setTimer(VertxKinesisOrchestraOptions.DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS * 2) {
                removeRedisToxies()
            }

            sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
            sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        }

    @Test
    internal fun save_consumer_shard_sequence_downstream_issue(testContext: VertxTestContext) =
        asyncTest(testContext) {

            val sequenceNumber = "sequencenumber".asSequenceNumberAt()

            preventDataFromRedisPassingAfter(2)

            vertx.setTimer(VertxKinesisOrchestraOptions.DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS * 2) {
                removeRedisToxies()
            }

            sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
            sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        }

    @Test
    internal fun save_consumer_shard_sequence_closed_connection(testContext: VertxTestContext) =
        asyncTest(testContext) {

            val sequenceNumber = "sequencenumber".asSequenceNumberAt()

            closeConnectionToRedis()

            vertx.setTimer(VertxKinesisOrchestraOptions.DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS * 3) {
                removeRedisToxies()
            }

            sut.saveConsumerShardSequenceNumber(shardId, sequenceNumber)
            sut.getConsumerShardSequenceNumber(shardId).shouldBe(sequenceNumber)
        }

    @Test
    internal fun concurrent_merge_resharding_event_count_closed_connection(testContext: VertxTestContext) =
        asyncTest(testContext, 100) { checkpoint ->
            val childShardId = ShardIdGenerator.generateShardId()
            val jobCount = 100
            repeat(jobCount) { jobNumber ->
                defaultTestScope.launch {
                    sut.getMergeReshardingEventCount(childShardId).shouldBeGreaterThan(0)
                    checkpoint.flag()
                }
                if (jobNumber == 50) {
                    closeConnectionToRedis()
                }
            }

            vertx.setTimer(VertxKinesisOrchestraOptions.DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS * 3) {
                removeRedisToxies()
            }
        }
}