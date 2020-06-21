package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asSequenceNumberAt
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.redis.client.RedisAPI
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.coroutines.resume

internal class ShardStatePersistenceTest : AbstractRedisTest() {

    companion object {
        private const val APPLICATION_NAME = "test-application"
        private const val STREAM_NAME = "test-stream"

        private const val DEFAULT_TEST_EXPIRATION = 1000L

        private val shardId = ShardIdGenerator.generateShardId()
    }

    private val sut by lazy {
        ShardStatePersistence(
            RedisAPI.api(redisClient),
            Duration.ofMillis(DEFAULT_TEST_EXPIRATION),
            RedisKeyFactory(APPLICATION_NAME, STREAM_NAME)
        )
    }

    @Test
    internal fun flagShardInProgress(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.isShardInProgress(shardId).shouldBeTrue()
    }

    @Test
    internal fun swapshard_progress_flag(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.isShardInProgress(shardId).shouldBeTrue()
        sut.flagShardNoMoreInProgress(shardId).shouldBeTrue()
        sut.isShardInProgress(shardId).shouldBeFalse()
    }

    @Test
    internal fun shard_progress_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        sut.flagShardInProgress(shardId).shouldBeTrue()
        sut.isShardInProgress(shardId).shouldBeTrue()
        delay(DEFAULT_TEST_EXPIRATION * 2)
        sut.isShardInProgress(shardId).shouldBeFalse()
    }

    @Test
    internal fun start_shard_progress_keepalive(testContext: VertxTestContext) = asyncTest(testContext) {
        val scheduleId = sut.startShardProgressAndKeepAlive(
            vertx, defaultTestScope,
            shardId
        )
        suspendCancellableCoroutine<Unit> {
            GlobalScope.launch {
                delay(DEFAULT_TEST_EXPIRATION * 3)
                it.resume(Unit)
            }
        }
        sut.isShardInProgress(shardId).shouldBeTrue()
        vertx.cancelTimer(scheduleId.id)
        delay(DEFAULT_TEST_EXPIRATION * 2)
        sut.isShardInProgress(shardId).shouldBeFalse()
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
        shardIds.forEach { sut.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION) }
        sut.getFinishedShardIds().shouldContainExactlyInAnyOrder(shardIds)
        val foundShardIds = sut.getFinishedShardIds()
        foundShardIds.shouldContainExactlyInAnyOrder(shardIds)
    }

    @Test
    internal fun save_finished_shard_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardIds = ShardIdGenerator.generateShardIdList(10)
        shardIds.forEach { sut.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION) }
        sut.getFinishedShardIds().shouldContainExactlyInAnyOrder(shardIds)
        delay(DEFAULT_TEST_EXPIRATION * 2)
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

    @Test
    internal fun get_shardids_in_progress_no_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            sut.getShardIdsInProgress().shouldBeEmpty()
        }

    @Test
    internal fun get_shardids_in_progress_one_shard_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            sut.flagShardInProgress(shardId).shouldBeTrue()
            sut.getShardIdsInProgress().shouldContainExactly(shardId)
        }

    @Test
    internal fun get_shardids_in_progress_many_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardIdList = mutableListOf(shardId).apply {
                repeat(100) {
                    val shardNumber = it + 1
                    add(ShardIdGenerator.generateShardId(shardNumber))
                }
            }
            shardIdList.forEach { shardId ->
                sut.flagShardInProgress(shardId).shouldBeTrue()
            }

            sut.getShardIdsInProgress().shouldContainExactlyInAnyOrder(shardIdList)
        }
}
