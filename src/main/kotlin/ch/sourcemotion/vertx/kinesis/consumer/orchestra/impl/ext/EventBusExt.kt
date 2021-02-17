package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageConsumer

internal fun <T> Message<T>.ack() = reply(null)

/**
 * Can be remove when https://github.com/vert-x3/vertx-lang-kotlin/issues/190 is solved.
 */
fun MessageConsumer<*>.completion(): Future<Void> {
    val promise = Promise.promise<Void>()
    completionHandler(promise::handle)
    return promise.future()
}