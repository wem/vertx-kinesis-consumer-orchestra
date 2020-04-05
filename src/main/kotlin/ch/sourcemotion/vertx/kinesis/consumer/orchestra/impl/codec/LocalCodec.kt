package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

/**
 * Codec that only supports local transport and not wire.
 */
internal open class LocalCodec<T : Any>(private val name: String) : MessageCodec<T, T> {

    companion object {
        inline fun <reified T : Any> create(): LocalCodec<T> = LocalCodec("${T::class.java.name}_codec")
    }

    /**
     * We currently not transfer any data over the wire.
     */
    override fun decodeFromWire(pos: Int, buffer: Buffer): T =
        throw UnsupportedOperationException("This codec is ned designed to use in clustered mode")

    /**
     * We currently not transfer any data over the wire.
     */
    override fun encodeToWire(buffer: Buffer, s: T) {
        throw UnsupportedOperationException("This codec is ned designed to use in clustered mode")
    }


    override fun systemCodecID(): Byte = -1
    override fun transform(s: T): T = s
    override fun name() = name
}
