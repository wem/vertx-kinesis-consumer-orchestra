package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import io.vertx.redis.client.Response

/**
 * As many Redis calls returns OK on success, we acknowledge that as boolean.
 */
fun Response?.okResponseAsBoolean() = toString().equals("OK", true)
