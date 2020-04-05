package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import io.vertx.kotlin.redis.client.scriptAwait
import io.vertx.redis.client.RedisAPI

/**
 * Simple a loader with cache of LUA scripts used for short, fast operations against Redis.
 */
object LuaScriptLoader {

    private val scriptCache = HashMap<LuaScriptDescription, String>()

    suspend fun loadScriptSha(scriptDescription: LuaScriptDescription, redisAPI: RedisAPI): String {
        return scriptCache.getOrPut(scriptDescription) {
            val scriptContent = LuaScriptLoader::class.java.getResourceAsStream(scriptDescription.path)
                .use { ins -> ins.bufferedReader(Charsets.UTF_8).use { reader -> reader.readText() } }
            return loadScriptRedis(scriptContent, redisAPI)
        }
    }

    private suspend fun loadScriptRedis(scriptContent: String, redisAPI: RedisAPI): String {
        return redisAPI.scriptAwait(listOf("LOAD", scriptContent)).toString()
    }
}
