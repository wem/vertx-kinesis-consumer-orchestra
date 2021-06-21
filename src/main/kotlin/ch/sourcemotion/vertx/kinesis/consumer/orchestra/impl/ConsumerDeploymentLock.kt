package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isFalse
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.DefaultLuaScriptDescription
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua.LuaExecutor
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import kotlinx.coroutines.delay
import mu.KLogging
import java.time.Duration

/**
 * For some reasons we need to do (re)deployment of shard consumer(s) in locked manner.
 * The most important reason is to avoid parallel shard processing. On the other hand, other approaches would be
 * much more complex and may even lesser reliable.
 *
 * This implementation is a partially adaption of the suggested lock behavior of Redis, called red lock.
 */
internal class ConsumerDeploymentLock(
    private val redis: Redis,
    private val luaExecutor: LuaExecutor,
    private val redisKeyFactory: RedisKeyFactory,
    private val lockExpiration: Duration,
    private val lockRetryInterval: Duration = Duration.ofMillis(500)
) {
    internal companion object : KLogging()

    suspend inline fun doLocked(block: () -> Unit) {
        val deploymentKey = redisKeyFactory.createDeploymentLockKey()
        val keys = listOf(deploymentKey)
        val args = listOf(lockExpiration.toMillis().toString())

        runCatching { acquireLock(deploymentKey, keys, args) }
            .getOrElse { throw VertxKinesisConsumerOrchestraException("Failed to obtain consumer deployment lock", it) }

        val taskResult = runCatching { block() }

        if (releaseLock(deploymentKey).isFalse()) {
            logger.warn {
                "Unable to release deployment lock: \"$deploymentKey\". " +
                        "In worst case, this may result in simultaneous consumers of a single shard"
            }
        }

        taskResult.getOrThrow()
    }

    private suspend fun acquireLock(deploymentKey: String, luaKeys: List<String>, luaArgs: List<String>) {
        val lockAcquisitionResponse = luaExecutor.execute(
            DefaultLuaScriptDescription.ACQUIRE_DEPLOYMENT_LOCK, keys = luaKeys, args = luaArgs
        )
        if (lockAcquisitionResponse.okResponseAsBoolean().isFalse()) {
            logger.info { "Unable to acquire lock on $deploymentKey will wait for $lockRetryInterval and retry" }
            delay(lockRetryInterval.toMillis())
            acquireLock(deploymentKey, luaKeys, luaArgs)
        }
    }

    private suspend fun releaseLock(deploymentKey: String) =
        redis.send(Request.cmd(Command.DEL).arg(deploymentKey)).await()?.toInteger() == 1
}
