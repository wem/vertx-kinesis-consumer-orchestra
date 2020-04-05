package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotlintest.matchers.boolean.shouldBeTrue
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.Request
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit

internal class ConsumerDeploymentLockTest : AbstractRedisTest() {

    private companion object {
        const val TEST_STREAM_NAME = "test-stream"
        const val TEST_APPLICATION_NAME = "test-application"

        val redisKeyFactory = RedisKeyFactory(TEST_APPLICATION_NAME, TEST_STREAM_NAME)
    }

    private val sut: ConsumerDeploymentLock by lazy {
        val redisApi = RedisAPI.api(redisClient)
        ConsumerDeploymentLock(
            redisApi,
            LuaExecutor(redisApi),
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

    @Timeout(value = 400, timeUnit = TimeUnit.MILLISECONDS)
    @Test
    internal fun lock_acquisition_waits_until_available(testContext: VertxTestContext) =
        asyncTest(testContext, 1) { checkpoint ->
            val testLockDuration = Duration.ofMillis(200)

            // We acquire the lock for 300ms, so the consumer deployment lock has to wait.
            acquireTestDeploymentLock(testLockDuration).shouldBeTrue()

            sut.doLocked {
                checkpoint.flag()
            }
        }

    @Test
    internal fun lock_acquired_when_do_within_lock(testContext: VertxTestContext) =
        asyncTest(testContext, 1) { checkpoint ->
            sut.doLocked {
                deploymentLockAcquired().shouldBeTrue()
                checkpoint.flag()
            }
        }

    private suspend fun acquireTestDeploymentLock(duration: Duration) = redisClient.sendAwait(
        Request.cmd(Command.SET)
            .arg(redisKeyFactory.createDeploymentLockKey())
            .arg("1")
            .arg("PX")
            .arg(duration.toMillis().toString())
    ).okResponseAsBoolean()

    private suspend fun deploymentLockAcquired() = redisClient.sendAwait(
        Request.cmd(Command.GET)
            .arg(redisKeyFactory.createDeploymentLockKey())
    )?.toInteger() == 1
}
