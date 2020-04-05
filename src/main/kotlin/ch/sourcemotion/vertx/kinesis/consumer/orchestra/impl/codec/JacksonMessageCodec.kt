package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec

import io.vertx.core.buffer.Buffer
import io.vertx.core.json.jackson.DatabindCodec
import kotlin.reflect.KClass

internal class JacksonMessageCodec<T : Any>(
    private val clazz: KClass<T>,
    name: String
) : LocalCodec<T>(name) {

    companion object {
        inline fun <reified T : Any> create(name: String): JacksonMessageCodec<T> =
            JacksonMessageCodec(T::class, name)
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): T {
        val length = buffer.getInt(pos)
        val finalPos = pos + 4
        return DatabindCodec.mapper().readValue(buffer.slice(finalPos, finalPos + length).bytes, clazz.java)
    }

    override fun encodeToWire(buffer: Buffer, s: T) {
        val encoded = DatabindCodec.mapper().writeValueAsBytes(s)
        buffer.appendInt(encoded.size)
        buffer.appendBytes(encoded)
    }
}
