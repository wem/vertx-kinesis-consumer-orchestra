package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.Request
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class AbstractRedisTest : AbstractVertxTest() {
    companion object {
        @JvmStatic
        @Container
        val redisContainer =
            KGenericContainer.createRedisContainer()

        private fun getRedisServerHost() = redisContainer.containerIpAddress
        private fun getRedisServerPort() = redisContainer.firstMappedPort
    }

    protected lateinit var redisClient: Redis
    protected lateinit var redisOptions: RedisOptions

    @BeforeEach
    internal fun setUpAbstractRedisClientTest() {
        redisOptions = RedisOptions()
            .setConnectionString("redis://${getRedisServerHost()}:${getRedisServerPort()}")
        eventBus = vertx.eventBus()
        redisClient = Redis.createClient(vertx, redisOptions)
    }

    @AfterEach
    internal fun cleanRedisCache(testContext: VertxTestContext) = asyncTest(testContext) {
        redisClient.sendAwait(Request.cmd(Command.FLUSHALL))
    }
}
