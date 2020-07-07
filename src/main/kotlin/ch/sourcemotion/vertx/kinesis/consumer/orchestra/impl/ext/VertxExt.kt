package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import io.vertx.core.Vertx
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume

suspend fun Vertx.setTimerAwait(delay: Long): Long {
    return suspendCancellableCoroutine { cont ->
        setTimer(delay) { timerId ->
            cont.resume(timerId)
        }
    }
}
