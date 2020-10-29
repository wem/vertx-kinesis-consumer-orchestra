package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import kotlin.contracts.contract

fun Boolean?.isTrue(): Boolean {
    contract {
        returns(true) implies (this@isTrue != null)
    }
    return this == true
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
