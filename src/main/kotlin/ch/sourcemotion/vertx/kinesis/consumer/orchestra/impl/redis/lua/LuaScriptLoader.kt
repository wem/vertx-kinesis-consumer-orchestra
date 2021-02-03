package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import io.vertx.kotlin.redis.client.sendAwait
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request

/**
 * Simple a loader with cache of LUA scripts used for short, fast operations against Redis.
 */
internal object LuaScriptLoader {

    private val scriptCache = HashMap<LuaScriptDescription, String>()

    suspend fun loadScriptSha(scriptDescription: LuaScriptDescription, redis: Redis): String {
        return scriptCache.getOrPut(scriptDescription) {
            val scriptContent = LuaScriptLoader::class.java.getResourceAsStream(scriptDescription.path)
                .use { ins -> ins.bufferedReader(Charsets.UTF_8).use { reader -> reader.readText() } }
            return loadScriptRedis(scriptContent, redis)
        }
    }

    private suspend fun loadScriptRedis(scriptContent: String, redis: Redis): String {
        return redis.sendAwait(Request.cmd(Command.SCRIPT).arg("LOAD").arg(scriptContent)).toString()
    }
}
