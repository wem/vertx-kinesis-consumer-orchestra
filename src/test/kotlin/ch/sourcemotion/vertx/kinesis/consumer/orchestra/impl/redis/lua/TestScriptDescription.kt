package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.lua

enum class TestScriptDescription(override val path: String) : LuaScriptDescription {
    HELLO_WORLD("/fixtures/lua/hello-world.lua"),
    RETURN_HELLO_WORLD_ARRAY("/fixtures/lua/return-hello-world-array.lua"),
    RETURN_KEY("/fixtures/lua/return-key.lua"),
    RETURN_ARG("/fixtures/lua/return-args.lua")
}
