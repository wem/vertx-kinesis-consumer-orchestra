package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.vertx.core.Vertx
import kotlinx.coroutines.future.await
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.math.BigInteger

fun Vertx.shareKinesisAsyncClientFactory(kinesisEndpointOverride: String) {
    val kinesisAsyncClientFactory =
        KinesisAsyncClientFactory(this, Localstack.region.id(), kinesisEndpointOverride)
    SharedData.shareInstance(this, kinesisAsyncClientFactory, KinesisAsyncClientFactory.SHARED_DATA_REF)
}

suspend fun KinesisAsyncClient.createAndGetStreamDescriptionWhenActive(
    shardCount: Int,
    streamName: String = TEST_STREAM_NAME
): StreamDescription {
    createStream {
        it.streamName(streamName).shardCount(shardCount)
    }.await()
    return streamDescriptionWhenActiveAwait(streamName)
}

suspend fun KinesisAsyncClient.putRecords(
    recordBunching: RecordPutBunching,
    streamName: String = TEST_STREAM_NAME,
    recordDataSupplier: (Int) -> SdkBytes = { count -> SdkBytes.fromUtf8String("record-data-$count") },
    partitionKeySupplier: (Int) -> String = { "partition-key_$it" }
) {
    repeat(recordBunching.recordBunches) { bunchIdx ->
        // Partition key is per bundle
        val partitionKey = partitionKeySupplier(bunchIdx)

        val putRequestRecords = List(recordBunching.recordsPerBunch) { recordIdx ->
            PutRecordsRequestEntry.builder().partitionKey(partitionKey).data(recordDataSupplier(recordIdx))
                .build()
        }
        val putResponse = putRecords {
            it.records(putRequestRecords).streamName(streamName)
        }.await()
        putResponse.failedRecordCount().shouldBe(0)
    }
}

suspend fun KinesisAsyncClient.putRecordsExplicitHashKey(
    recordBunching: RecordPutBunching,
    recordDataSupplier: (Int) -> SdkBytes = { count -> SdkBytes.fromUtf8String("record-data-$count") },
    streamName: String = TEST_STREAM_NAME,
    predefinedShards: ShardList? = null
) {
    // Count of record bundles must equal to the count of shards
    val shards = predefinedShards ?: streamDescriptionWhenActiveAwait(streamName).shards()
    shards.shouldHaveSize(recordBunching.recordBunches)

    repeat(recordBunching.recordBunches) { bundleIdx ->
        val hashKey = shards[bundleIdx].hashKeyRange().startingHashKey()

        val putRequestRecords = List(recordBunching.recordsPerBunch) { recordIdx ->
            PutRecordsRequestEntry.builder().explicitHashKey(hashKey).partitionKey("partition-key")
                .data(recordDataSupplier(recordIdx))
                .build()
        }.toList()
        val putResponse = putRecords {
            it.records(putRequestRecords).streamName(streamName)
        }.await()
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
}
