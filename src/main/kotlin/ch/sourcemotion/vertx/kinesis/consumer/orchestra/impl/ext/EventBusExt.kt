package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import io.vertx.core.eventbus.Message

internal fun <T> Message<T>.ack() = reply(null)
