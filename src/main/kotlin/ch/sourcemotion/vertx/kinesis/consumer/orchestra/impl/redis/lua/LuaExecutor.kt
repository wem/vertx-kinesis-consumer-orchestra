package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import io.vertx.kotlin.redis.client.evalshaAwait
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.Response

class LuaExecutor(private val redisApi: RedisAPI) {
    suspend fun execute(
        scriptDescription: LuaScriptDescription,
        keys: List<String> = emptyList(),
        args: List<String> = emptyList()
    ): Response? {
        val scriptSha = LuaScriptLoader.loadScriptSha(scriptDescription, redisApi)
        return redisApi.evalshaAwait(mutableListOf(scriptSha, keys.size.toString()).apply {
            addAll(keys)
            addAll(args)
        })
    }
}
