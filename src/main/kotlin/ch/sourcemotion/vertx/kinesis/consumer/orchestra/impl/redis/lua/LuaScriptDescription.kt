package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

interface LuaScriptDescription {
    val path: String
}

enum class DefaultLuaScriptDescription(override val path: String) : LuaScriptDescription {
    ACQUIRE_DEPLOYMENT_LOCK("/lua/acquire-deployment-lock.lua")
}
