package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

internal interface LuaScriptDescription {
    val path: String
}

internal enum class DefaultLuaScriptDescription(override val path: String) : LuaScriptDescription {
    ACQUIRE_DEPLOYMENT_LOCK("/lua/acquire-deployment-lock.lua"),
    SET_VALUE_RETURN_KEY_COUNT_BY_PATTERN("/lua/set-value-return-key-count-by-pattern.lua"),
    DELETE_VALUES_BY_KEY_PATTERN_RETURN_DELETED_COUNT("/lua/delete-values-by-key-pattern-return-deleted-count.lua"),
}
