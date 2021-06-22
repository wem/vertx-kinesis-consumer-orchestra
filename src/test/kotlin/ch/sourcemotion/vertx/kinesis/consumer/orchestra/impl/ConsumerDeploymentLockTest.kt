package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import java.time.Duration

internal class ConsumerDeploymentLockTest : AbstractRedisTest() {

    private companion object {
        const val TEST_STREAM_NAME = "test-stream"
        const val TEST_APPLICATION_NAME = "test-application"

        val redisKeyFactory = RedisKeyFactory(TEST_APPLICATION_NAME, TEST_STREAM_NAME)
    }

    private val sut: ConsumerDeploymentLock by lazy {
        ConsumerDeploymentLock(
            redisClient,
            LuaExecutor(redisClient),
            redisKeyFactory,
            Duration.ofMillis(100),
            Duration.ofMillis(100)
        )
    }


    @Test
    internal fun lock_acquisition_immediately_possible(testContext: VertxTestContext) =
        asyncTest(testContext, 1) { checkpoint ->
            sut.doLocked {
                checkpoint.flag()
            }
        }

    @Test
    internal fun lock_acquisition_waits_until_available(testContext: VertxTestContext) =
        asyncTest(testContext, 2) { checkpoint ->
            var earlierLockReleased = false
            launch {
                delay(10)
                sut.doLocked {
                    testContext.verify { earlierLockReleased.shouldBeTrue() }
                    checkpoint.flag()
                }
            }
            launch {
                sut.doLocked {
                    delay(10)
                    checkpoint.flag()
                }
                earlierLockReleased = true
            }
        }

    @Test
    internal fun lock_acquired_flag_set(testContext: VertxTestContext) =
        asyncTest(testContext, 1) { checkpoint ->
            sut.doLocked {
                deploymentLockAcquired().shouldBeTrue()
                checkpoint.flag()
            }
        }

    @Test
    internal fun pending_locks(testContext: VertxTestContext) = testContext.async(10) { checkpoint ->
        var currentLocks = 0
        repeat(10) {
            launch {
                sut.doLocked {
                    currentLocks.shouldBe(0)
                    ++currentLocks
                    delay(10)
                    currentLocks.shouldBe(1)
                    --currentLocks
                    checkpoint.flag()
                }
            }
        }
    }

    @Test
    internal fun exception_within_lock_block(testContext: VertxTestContext) = testContext.async {
        val exceptionMsg = "Test exception"
        val cause = shouldThrow<Exception> {
            sut.doLocked {
                throw Exception(exceptionMsg)
            }
        }
        deploymentLockAcquired().shouldBeFalse()
        cause.message.shouldBe(exceptionMsg)
    }

    private suspend fun deploymentLockAcquired() = redisClient.sendAwait(
        Request.cmd(Command.GET)
            .arg(redisKeyFactory.createDeploymentLockKey())
    )?.toInteger() == 1
}
