package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import software.amazon.awssdk.services.kinesis.model.Shard

typealias ShardList = List<Shard>
typealias ShardIdList = List<ShardId>

data class ShardIterator(val iter: String) {
    override fun toString() = iter
}

fun String.asShardIteratorTyped() = ShardIterator(this)

data class ShardId(val id: String) {
    override fun toString() = id
}

fun String.asShardIdTyped(): ShardId = ShardId(this)

inline class TimerId(val id: Long)

fun Long.asTimerIdTyped() = TimerId(this)
