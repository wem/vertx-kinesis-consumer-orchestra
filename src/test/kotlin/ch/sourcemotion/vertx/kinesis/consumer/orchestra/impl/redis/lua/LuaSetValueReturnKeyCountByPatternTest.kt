package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.redis.client.RedisAPI
import org.junit.jupiter.api.Test
import kotlin.LazyThreadSafetyMode.NONE

class LuaSetValueReturnKeyCountByPatternTest : AbstractRedisTest() {

    private val luaExecutor by lazy(NONE) { LuaExecutor(RedisAPI.api(redisClient)) }

    @Test
    internal fun set_value_once(testContext: VertxTestContext) = asyncTest(testContext) {
        val value = "1"
        val key = "some-key"
        val pattern = "some-*"

        val response = luaExecutor.execute(
            DefaultLuaScriptDescription.SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN,
            listOf(key),
            listOf(value, pattern)
        )
        response.shouldNotBeNull()
        response.toInteger().shouldBe(1)
    }

    @Test
    internal fun set_same_value_twice(testContext: VertxTestContext) = asyncTest(testContext) {
        val value = "1"
        val key = "some-key"
        val pattern = "some-*"

        val response = luaExecutor.execute(
            DefaultLuaScriptDescription.SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN,
            listOf(key),
            listOf(value, pattern)
        )
        response.shouldNotBeNull()
        response.toInteger().shouldBe(1)

        val secondResponse = luaExecutor.execute(
            DefaultLuaScriptDescription.SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN,
            listOf(key),
            listOf(value, pattern)
        )
        secondResponse.shouldNotBeNull()
        secondResponse.toInteger().shouldBe(1)
    }

    @Test
    internal fun set_different_keys(testContext: VertxTestContext) = asyncTest(testContext) {
        val value = "1"
        val key = "some-key"
        val otherKey = "other-key"
        val pattern = "*-key"

        val parentResponse = luaExecutor.execute(
            DefaultLuaScriptDescription.SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN,
            listOf(key),
            listOf(value, pattern)
        )
        parentResponse.shouldNotBeNull()
        parentResponse.toInteger().shouldBe(1)

        val adjacentParentResponse = luaExecutor.execute(
            DefaultLuaScriptDescription.SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN,
            listOf(otherKey),
            listOf(value, pattern)
        )
        adjacentParentResponse.shouldNotBeNull()
        adjacentParentResponse.toInteger().shouldBe(2)
    }
}
