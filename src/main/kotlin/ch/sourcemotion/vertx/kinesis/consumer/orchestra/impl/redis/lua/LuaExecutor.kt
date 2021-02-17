package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import io.vertx.redis.client.Response

internal class LuaExecutor(private val redis: Redis) {
    suspend fun execute(
        scriptDescription: LuaScriptDescription,
        keys: List<String> = emptyList(),
        args: List<String> = emptyList()
    ): Response? {
        val scriptSha = LuaScriptLoader.loadScriptSha(scriptDescription, redis)
        val scriptArgs = ArrayList(keys).apply { addAll(args) }
        val command = Request.cmd(Command.EVALSHA).arg(scriptSha).arg("${keys.size}")
        scriptArgs.forEach { scriptArg -> command.arg(scriptArg) }
        return redis.send(command).await()
    }
}
