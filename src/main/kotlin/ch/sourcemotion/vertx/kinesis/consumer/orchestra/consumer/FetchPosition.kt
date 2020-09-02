package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SequenceNumber
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIterator

/**
 * Position in the Kinesis stream to fetch. Usually the iterator is used, except in the case of fetching failure (e.g. iterator got expired).
 * Then a fresh iterator will be queried according the sequence number.
 */
data class FetchPosition(val iterator: ShardIterator, val sequenceNumber: SequenceNumber?)
