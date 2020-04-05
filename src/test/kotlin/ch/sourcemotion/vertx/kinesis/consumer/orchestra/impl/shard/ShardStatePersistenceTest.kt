package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIteratorTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotlintest.matchers.boolean.shouldBeFalse
import io.kotlintest.matchers.boolean.shouldBeTrue
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.matchers.collections.shouldContainExactly
import io.kotlintest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotlintest.matchers.types.shouldBeNull
import io.kotlintest.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.redis.client.RedisAPI
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*
import kotlin.coroutines.resume

internal class ShardStatePersistenceTest : AbstractRedisTest() {

    companion object {
        private const val APPLICATION_NAME = "test-application"
        private const val STREAM_NAME = "test-stream"

        private const val DEFAULT_TEST_EXPIRATION = 1000L

        private val shardId = ShardIdGenerator.generateShardId()
    }


    @Test
    internal fun flagShardInProgress(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardStatePersistence = createShardStatePersistence()
        shardStatePersistence.flagShardInProgress(shardId).shouldBeTrue()
        shardStatePersistence.isShardInProgress(shardId).shouldBeTrue()
    }

    @Test
    internal fun swapshard_progress_flag(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardStatePersistence = createShardStatePersistence()
        shardStatePersistence.flagShardInProgress(shardId).shouldBeTrue()
        shardStatePersistence.isShardInProgress(shardId).shouldBeTrue()
        shardStatePersistence.flagShardNoMoreInProgress(shardId).shouldBeTrue()
        shardStatePersistence.isShardInProgress(shardId).shouldBeFalse()
    }

    @Test
    internal fun shard_progress_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardStatePersistence = createShardStatePersistence()
        shardStatePersistence.flagShardInProgress(shardId).shouldBeTrue()
        shardStatePersistence.isShardInProgress(shardId).shouldBeTrue()
        delay(DEFAULT_TEST_EXPIRATION * 2)
        shardStatePersistence.isShardInProgress(shardId).shouldBeFalse()
    }

    @Test
    internal fun start_shard_progress_keepalive(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardStatePersistence = createShardStatePersistence()
        val scheduleId = shardStatePersistence.startShardProgressAndKeepAlive(
            vertx, defaultTestScope,
            shardId
        )
        suspendCancellableCoroutine<Unit> {
            GlobalScope.launch {
                delay(DEFAULT_TEST_EXPIRATION * 3)
                it.resume(Unit)
            }
        }
        shardStatePersistence.isShardInProgress(shardId).shouldBeTrue()
        vertx.cancelTimer(scheduleId.id)
        delay(DEFAULT_TEST_EXPIRATION * 2)
        shardStatePersistence.isShardInProgress(shardId).shouldBeFalse()
    }

    @Test
    internal fun save_and_get_shardIterator(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardIterator = UUID.randomUUID().toString().asShardIteratorTyped()
            val shardStatePersistence = createShardStatePersistence()
            shardStatePersistence.saveShardIterator(shardId, shardIterator)
            shardStatePersistence.getShardIterator(shardId).shouldBe(shardIterator)
        }

    @Test
    internal fun delete_shard_iterator(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardIterator = UUID.randomUUID().toString().asShardIteratorTyped()
        val shardStatePersistence = createShardStatePersistence()
        shardStatePersistence.saveShardIterator(shardId, shardIterator)
        shardStatePersistence.getShardIterator(shardId).shouldBe(shardIterator)
        shardStatePersistence.deleteShardIterator(shardId).shouldBeTrue()
        shardStatePersistence.getShardIterator(shardId).shouldBeNull()
    }

    @Test
    internal fun save_finished_shard(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardStatePersistence = createShardStatePersistence()
        val shardIds = ShardIdGenerator.generateShardIdList(2)
        shardIds.forEach { shardStatePersistence.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION) }
        shardIds.forEach { shardStatePersistence.isShardFinished(it).shouldBeTrue() }
        val foundShardIds = shardStatePersistence.getFinishedShardIds()
        foundShardIds.shouldContainExactlyInAnyOrder(shardIds)
    }

    @Test
    internal fun save_finished_shard_expiration(testContext: VertxTestContext) = asyncTest(testContext) {
        val shardStatePersistence = createShardStatePersistence()
        val shardIds = ShardIdGenerator.generateShardIdList(10)
        shardIds.forEach { shardStatePersistence.saveFinishedShard(it, DEFAULT_TEST_EXPIRATION) }
        shardIds.forEach { shardStatePersistence.isShardFinished(it).shouldBeTrue() }
        delay(DEFAULT_TEST_EXPIRATION * 2)
        shardStatePersistence.getFinishedShardIds().shouldBeEmpty()
    }

    @Test
    internal fun get_shardids_in_progress_no_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardStatePersistence = createShardStatePersistence(DEFAULT_TEST_EXPIRATION)
            shardStatePersistence.getShardIdsInProgress().shouldBeEmpty()
        }

    @Test
    internal fun get_shardids_in_progress_one_shard_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardStatePersistence = createShardStatePersistence(DEFAULT_TEST_EXPIRATION)
            shardStatePersistence.flagShardInProgress(shardId).shouldBeTrue()
            shardStatePersistence.getShardIdsInProgress().shouldContainExactly(shardId)
        }

    @Test
    internal fun get_shardids_in_progress_many_shards_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shardStatePersistence = createShardStatePersistence(DEFAULT_TEST_EXPIRATION)
            val shardIdList = mutableListOf(shardId).apply {
                repeat(100) {
                    val shardNumber = it + 1
                    add(ShardIdGenerator.generateShardId(shardNumber))
                }
            }
            shardIdList.forEach { shardId ->
                shardStatePersistence.flagShardInProgress(shardId).shouldBeTrue()
            }

            shardStatePersistence.getShardIdsInProgress().shouldContainExactlyInAnyOrder(shardIdList)
        }

    private fun createShardStatePersistence(processExpirationMillis: Long = DEFAULT_TEST_EXPIRATION) =
        ShardStatePersistence(
            RedisAPI.api(redisClient),
            Duration.ofMillis(processExpirationMillis),
            RedisKeyFactory(APPLICATION_NAME, STREAM_NAME)
        )
}
