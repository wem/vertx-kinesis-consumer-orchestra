package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.concurrent.CompletableFuture
import kotlin.contracts.contract
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

fun Boolean?.isTrue(): Boolean {
    contract {
        returns(true) implies (this@isTrue != null)
    }
    return this != null && this
}

fun Any?.isNotNull(): Boolean {
    contract {
        returns(true) implies (this@isNotNull != null)
    }
    return this != null
}

fun Any?.isNull(): Boolean {
    contract {
        returns(true) implies (this@isNull == null)
    }
    return this == null
}

fun Boolean?.isFalse(): Boolean {
    return this == null || !this
}

fun CharSequence?.isNotNullOrBlank(): Boolean {
    contract {
        returns(true) implies (this@isNotNullOrBlank != null)
    }
    return this.isNullOrBlank().isFalse()
}

suspend fun <T> CompletableFuture<T>.awaitSuspending(): T {
    return suspendCancellableCoroutine { cont ->
        whenCompleteAsync { value, throwable ->
            throwable?.let { cont.resumeWithException(it) } ?: cont.resume(value)
        }
    }
}
