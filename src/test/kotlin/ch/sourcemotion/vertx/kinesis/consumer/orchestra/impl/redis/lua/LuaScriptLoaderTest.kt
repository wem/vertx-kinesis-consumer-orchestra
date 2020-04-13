package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import io.kotest.matchers.string.shouldHaveLength
import io.vertx.junit5.VertxTestContext
import io.vertx.redis.client.RedisAPI
import org.junit.jupiter.api.Test

internal class LuaScriptLoaderTest : AbstractRedisTest() {

    @Test
    internal fun load_hello_world_script(testContext: VertxTestContext) = asyncTest(testContext) {
        val helloWorldScriptSha =
            LuaScriptLoader.loadScriptSha(TestScriptDescription.HELLO_WORLD, RedisAPI.api(redisClient))
                .replace("\n", "")
        // It's the sha1 of the script content
        helloWorldScriptSha.shouldHaveLength(40)
    }
}
