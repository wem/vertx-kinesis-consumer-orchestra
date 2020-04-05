package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import io.vertx.core.eventbus.Message

fun <T> Message<T>.ack() = reply(null)
