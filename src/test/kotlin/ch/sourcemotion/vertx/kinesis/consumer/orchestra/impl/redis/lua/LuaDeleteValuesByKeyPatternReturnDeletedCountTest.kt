package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.okResponseAsBoolean
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import org.junit.jupiter.api.Test
import kotlin.LazyThreadSafetyMode.NONE

class LuaDeleteValuesByKeyPatternReturnDeletedCountTest : AbstractRedisTest() {

    private val luaExecutor by lazy(NONE) { LuaExecutor(redisClient) }

    private val luaScript = DefaultLuaScriptDescription.DELETE_VALUES_BY_KEY_PATTERN_RETURN_DELETED_COUNT

    @Test
    internal fun single_key(testContext: VertxTestContext) = asyncTest(testContext) {
        val value = "1"
        val key = "some-key"
        val pattern = "some-*"

        redisClient.sendAwait(Request.cmd(Command.SET).arg(key).arg(value)).okResponseAsBoolean().shouldBeTrue()

        val luaResponse = luaExecutor.execute(luaScript, listOf(), listOf(pattern))
        luaResponse.shouldNotBeNull()
        luaResponse.toInteger().shouldBe(1)
    }

    @Test
    internal fun multi_key(testContext: VertxTestContext) = asyncTest(testContext) {
        val value = "1"
        val key = "some-key"
        val otherKey = "other-key"
        val pattern = "*-key"

        redisClient.sendAwait(Request.cmd(Command.SET).arg(key).arg(value)).okResponseAsBoolean().shouldBeTrue()
        redisClient.sendAwait(Request.cmd(Command.SET).arg(otherKey).arg(value)).okResponseAsBoolean().shouldBeTrue()

        val luaResponse = luaExecutor.execute(luaScript,listOf(),listOf(pattern))
        luaResponse.shouldNotBeNull()
        luaResponse.toInteger().shouldBe(2)
    }
}
