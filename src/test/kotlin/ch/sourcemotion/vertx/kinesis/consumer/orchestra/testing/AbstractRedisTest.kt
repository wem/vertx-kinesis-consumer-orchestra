package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.KGenericContainer.Companion.REDIS_PORT
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.Request
import kotlinx.coroutines.delay
import mu.KLogging
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.Network
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*

@Testcontainers
abstract class AbstractRedisTest : AbstractVertxTest() {
    companion object : KLogging() {
        @JvmStatic
        private val network = Network.SHARED

        @JvmStatic
        @Container
        val redisContainer = KGenericContainer.createRedisContainer(network)
    }

    @Container
    val toxiproxy = ToxiproxyContainer().withNetwork(network)

    val redisProxy by lazy { toxiproxy.getProxy(redisContainer, REDIS_PORT) }

    private fun getRedisServerHost() = redisProxy.containerIpAddress
    private fun getRedisServerPort() = redisProxy.proxyPort


    protected val redisOptions: RedisOptions by lazy {
        RedisOptions().setConnectionString("redis://${getRedisServerHost()}:${getRedisServerPort()}")
    }

    protected val redisClient: Redis by lazy { Redis.createClient(vertx, redisOptions) }

    @BeforeEach
    internal fun cleanRedisBeforeTest(testContext: VertxTestContext) = asyncTest(testContext) {
        removeRedisToxies()
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

    protected fun removeRedisToxies() {
        redisProxy.toxics().all.forEach { it.remove() }
    }

    /**
     * Just let pass number of [bytes] from Redis. Afterwards the bandwidth is set to 0.
     */
    protected fun preventDataFromRedisPassingAfter(bytes: Long) {
        redisProxy.toxics().limitData(UUID.randomUUID().toString(), ToxicDirection.UPSTREAM, bytes)
    }

    /**
     * Just let pass number of [bytes] to Redis. Afterwards the bandwidth is set to 0.
     */
    protected fun preventDataToRedisPassingAfter(bytes: Long) {
        redisProxy.toxics().limitData(UUID.randomUUID().toString(), ToxicDirection.DOWNSTREAM, bytes)
    }

    protected suspend fun closeConnectionToRedis() {
        redisProxy.toxics().timeout(UUID.randomUUID().toString(), ToxicDirection.DOWNSTREAM, 1)
        redisProxy.toxics().timeout(UUID.randomUUID().toString(), ToxicDirection.UPSTREAM, 1)
        delay(1)
    }
}
