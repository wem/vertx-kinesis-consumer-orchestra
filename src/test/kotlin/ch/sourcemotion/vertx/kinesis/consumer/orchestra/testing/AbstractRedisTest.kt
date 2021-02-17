package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.KGenericContainer.Companion.REDIS_PORT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension.SingletonContainer
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension.SingletonContainerExtension
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.Request
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.Network
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName
import java.util.*
import kotlin.LazyThreadSafetyMode.NONE

@ExtendWith(SingletonContainerExtension::class)
abstract class AbstractRedisTest(private val deployShardPersistence: Boolean = true) : AbstractVertxTest() {
    companion object : KLogging() {
        private val toxiProxyDockerImageName = DockerImageName.parse("shopify/toxiproxy:2.1.4")

        @JvmStatic
        private val network = Network.SHARED

        @JvmStatic
        @get:SingletonContainer
        val redisContainer = KGenericContainer.createRedisContainer(network)

        @JvmStatic
        @get:SingletonContainer
        val toxiproxy = ToxiproxyContainer(toxiProxyDockerImageName).withNetwork(network)

        val redisProxy by lazy { toxiproxy.getProxy(redisContainer, REDIS_PORT) }
    }

    private fun getRedisServerHost() = redisProxy.containerIpAddress
    private fun getRedisServerPort() = redisProxy.proxyPort

    protected val redisHeimdallOptions: RedisHeimdallOptions by lazy {
        RedisHeimdallOptions(RedisOptions().setConnectionString("redis://${getRedisServerHost()}:${getRedisServerPort()}"))
    }

    protected val redisClient: Redis by lazy(NONE) { RedisHeimdall.create(vertx, redisHeimdallOptions) }

    protected val shardStatePersistenceService: ShardStatePersistenceServiceAsync by lazy(NONE) {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    @BeforeEach
    internal fun deployShardStatePersistence(testContext: VertxTestContext) = asyncTest(testContext) {
        if (deployShardPersistence) {
            deployShardStatePersistenceService()
        }
    }

    @AfterEach
    internal fun cleanUpRedis(testContext: VertxTestContext) = testContext.async {
        removeRedisToxies()
        flushAllOnRedis()
    }

    private suspend fun flushAllOnRedis() {
        val tempClient = RedisHeimdall.create(vertx, redisHeimdallOptions)
        val flushResponseResult = runCatching { tempClient.send(Request.cmd(Command.FLUSHALL)).await() }
        tempClient.close()
        val flushResponse = flushResponseResult.getOrThrow()
        if (flushResponse.isNotNull() && flushResponse.toString() == "OK") {
            logger.info { "Redis server all flushed" }
        } else {
            logger.warn { "Flush Redis server may failed: \"${flushResponse?.toString()}\"" }
        }
    }

    protected fun CoroutineScope.removeRedisToxiesAfter(delayMillis: Long) {
        launch {
            delay(delayMillis)
            removeRedisToxies()
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
        delay(2)
    }

    protected suspend fun deployShardStatePersistenceService(
        shardProgressExpirationMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS
    ) {
        val options = RedisShardStatePersistenceServiceVerticleOptions(
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            redisHeimdallOptions,
            shardProgressExpirationMillis
        )
        deployTestVerticle<RedisShardStatePersistenceServiceVerticle>(options)
    }
}
