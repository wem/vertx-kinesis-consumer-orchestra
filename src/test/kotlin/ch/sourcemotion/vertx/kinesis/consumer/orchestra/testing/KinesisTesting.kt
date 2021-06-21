package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.KinesisClientOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.NettyKinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.vertx.core.Vertx
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.math.BigInteger

/**
 * Localstack simulates some latency on Kinesis API calls. This value must get considered on timing related tests.
 *
 * [500] is the default.
 */
const val KINESIS_API_LATENCY_MILLIS = 500

fun Vertx.shareKinesisAsyncClientFactory() {
    val factory = KinesisAsyncClientFactory(this, AWS_REGION, KinesisClientOptions())
    SharedData.shareInstance(this, factory, KinesisAsyncClientFactory.SHARED_DATA_REF)
}

fun Vertx.shareNettyKinesisAsyncClientFactory() {
    val factory = NettyKinesisAsyncClientFactory(this, AWS_REGION, KinesisClientOptions())
    SharedData.shareInstance(this, factory, NettyKinesisAsyncClientFactory.SHARED_DATA_REF)
}

suspend fun KinesisAsyncClient.createAndGetStreamDescriptionWhenActive(
    shardCount: Int = 1,
): StreamDescription {
    createStream {
        it.streamName(TEST_STREAM_NAME).shardCount(shardCount)
    }.await()
    return streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
}

suspend fun KinesisAsyncClient.putRecords(
    recordBatching: RecordPutBatching,
    recordDataSupplier: (Int) -> SdkBytes = { suffix -> SdkBytes.fromUtf8String("record-data-$suffix") },
    partitionKeySupplier: (Int) -> String = { "partition-key_$it" }
) {
    repeat(recordBatching.recordBatches) { bunchIdx ->
        // Partition key is per bundle
        val partitionKey = partitionKeySupplier(bunchIdx)

        val putRequestRecords = List(recordBatching.recordsPerBatch) { recordIdx ->
            PutRecordsRequestEntry.builder().partitionKey(partitionKey).data(recordDataSupplier(recordIdx))
                .build()
        }
        val putResponse = putRecords {
            it.records(putRequestRecords).streamName(TEST_STREAM_NAME)
        }.await()
        putResponse.failedRecordCount().shouldBe(0)
    }
}

suspend fun KinesisAsyncClient.putRecordsExplicitHashKey(
    recordBatching: RecordPutBatching,
    recordDataSupplier: (Int) -> SdkBytes = { suffix -> SdkBytes.fromUtf8String("record-data-$suffix") },
    predefinedShards: ShardList? = null
) {
    // Count of record bundles must be equal according the shards count
    val shards = predefinedShards ?: streamDescriptionWhenActiveAwait(TEST_STREAM_NAME).shards()
    shards.shouldHaveSize(recordBatching.recordBatches)

    repeat(recordBatching.recordBatches) { bundleIdx ->
        val shard = shards[bundleIdx]
        val putRequestRecords = List(recordBatching.recordsPerBatch) { recordIdx ->
            val hashKey = shard.hashKeyRange().startingHashKey()
            PutRecordsRequestEntry.builder()
                .explicitHashKey(hashKey)
                .partitionKey("1")
                .data(recordDataSupplier(recordIdx))
                .build()
        }.toList()

        val putResponse = putRecords {
            it.records(putRequestRecords).streamName(TEST_STREAM_NAME)
        }.await()
        putResponse.records().forEach { record ->
            record.shardId().shouldBe(shard.shardId())
        }
        putResponse.failedRecordCount().shouldBe(0)
    }
}

suspend fun KinesisAsyncClient.splitShardFair(shardToSplit: Shard) {
    // https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding-split.html
    splitShard {
        val startingHashKey = BigInteger(shardToSplit.hashKeyRange().startingHashKey())
        val endingHashKey = BigInteger(shardToSplit.hashKeyRange().endingHashKey())
        val newStartingHashKey = startingHashKey.add(endingHashKey).divide(BigInteger("2")).toString()
        it.streamName(TEST_STREAM_NAME).shardToSplit(shardToSplit.shardId())
            .newStartingHashKey(newStartingHashKey)
    }.await()
}

suspend fun KinesisAsyncClient.mergeShards(parentShard: Shard, adjacentShard: Shard) {
    // https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding-merge.html
    mergeShards {
        it.streamName(TEST_STREAM_NAME)
        it.shardToMerge(parentShard.shardId())
        it.adjacentShardToMerge(adjacentShard.shardId())
    }.await()
    streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
    delay(7000)
}

fun StreamDescription.shardIds() = shards().map { it.shardIdTyped() }

suspend fun KinesisAsyncClient.mergeShards(parentShards: List<Shard>) {
    if (parentShards.size != 2) {
        throw IllegalArgumentException("Only 2 parents can be merged. ${parentShards.size} is an illegal count of parents")
    }
    mergeShards(parentShards.first(), parentShards.last())
}
