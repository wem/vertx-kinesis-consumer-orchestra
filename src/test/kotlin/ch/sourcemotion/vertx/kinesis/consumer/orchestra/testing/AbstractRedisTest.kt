package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.Request
import mu.KLogging
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class AbstractRedisTest : AbstractVertxTest() {
    companion object : KLogging() {
        @JvmStatic
        @Container
        val redisContainer = KGenericContainer.createRedisContainer()

        private fun getRedisServerHost() = redisContainer.containerIpAddress
        private fun getRedisServerPort() = redisContainer.firstMappedPort
    }

    protected val redisOptions: RedisOptions by lazy {
        RedisOptions().setConnectionString("redis://${getRedisServerHost()}:${getRedisServerPort()}")
    }

    protected val redisClient: Redis by lazy { Redis.createClient(vertx, redisOptions) }

    @BeforeEach
    internal fun cleanRedisBeforeTest(testContext: VertxTestContext) = asyncTest(testContext) {
        flushAllOnRedisServer()
    }

    private suspend fun flushAllOnRedisServer() {
        val flushResponse = redisClient.sendAwait(Request.cmd(Command.FLUSHALL))
        if (flushResponse.isNotNull() && flushResponse.toString() == "OK") {
            logger.info { "Redis server all flushed" }
        } else {
            logger.warn { "Flush Redis server may failed: \"${flushResponse?.toString()}\"" }
        }
    }
}
