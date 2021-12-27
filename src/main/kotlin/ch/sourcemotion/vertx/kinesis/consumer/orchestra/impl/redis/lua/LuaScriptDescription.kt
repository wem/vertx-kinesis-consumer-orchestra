package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

internal interface LuaScriptDescription {
    val path: String
}

internal enum class DefaultLuaScriptDescription(override val path: String) : LuaScriptDescription {
    ACQUIRE_DEPLOYMENT_LOCK("/lua/acquire-deployment-lock.lua")
}
