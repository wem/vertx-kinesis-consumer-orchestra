package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList

class StartConsumersCmd(val shardIds: ShardIdList, val iteratorStrategy: ShardIteratorStrategy)
