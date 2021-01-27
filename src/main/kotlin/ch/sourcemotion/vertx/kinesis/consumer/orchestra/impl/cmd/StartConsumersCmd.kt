package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList

data class StartConsumersCmd(val shardIds: ShardIdList, val iteratorStrategy: ShardIteratorStrategy) {
    companion object FailureCodes {
        const val CONSUMER_CAPACITY_FAILURE = 1
        const val CONSUMER_START_FAILURE = 2
    }
}
