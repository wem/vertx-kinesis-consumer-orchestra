package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.redis.client.RedisAPI
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class LuaExecutorTest : AbstractRedisTest() {

    private lateinit var luaExecutor: LuaExecutor

    @BeforeEach
    internal fun setUpLuaExecutorTest() {
        luaExecutor = LuaExecutor(RedisAPI.api(redisClient))
    }

    @Test
    internal fun execute_hello_world(testContext: VertxTestContext) = asyncTest(testContext) {
        val expectedResponse = "Hello World"
        val response = luaExecutor.execute(TestScriptDescription.HELLO_WORLD)
        response.shouldNotBeNull()
        response.toString(Charsets.UTF_8).shouldBe(expectedResponse)
    }

    @Test
    internal fun execute_hello_world_array_return(testContext: VertxTestContext) = asyncTest(testContext) {
        val expectedResponse = "Hello World"
        val response = luaExecutor.execute(TestScriptDescription.RETURN_HELLO_WORLD_ARRAY)
        response.shouldNotBeNull()
        response.size().shouldBe(2)
        response[0].toString(Charsets.UTF_8).shouldBe(expectedResponse)
        response[1].toString(Charsets.UTF_8).shouldBe(expectedResponse)
    }

    @Test
    internal fun execute_return_key(testContext: VertxTestContext) = asyncTest(testContext) {
        val key = "some-key"
        val response = luaExecutor.execute(TestScriptDescription.RETURN_KEY, listOf(key))
        response.shouldNotBeNull()
        response.toString(Charsets.UTF_8).shouldBe(key)
    }

    @Test
    internal fun execute_return_arg(testContext: VertxTestContext) = asyncTest(testContext) {
        val arg = "some-arg"
        val response = luaExecutor.execute(TestScriptDescription.RETURN_ARG, emptyList(), listOf(arg))
        response.shouldNotBeNull()
        response.toString(Charsets.UTF_8).shouldBe(arg)
    }
}
