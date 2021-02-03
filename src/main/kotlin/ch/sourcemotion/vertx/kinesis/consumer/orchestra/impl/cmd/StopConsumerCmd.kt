package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId

internal data class StopConsumerCmd(val shardId: ShardId) {
    companion object {
        const val UNKNOWN_CONSUMER_FAILURE = 1
        const val CONSUMER_STOP_FAILURE = 2
    }
}
