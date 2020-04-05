package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.awaitSuspending
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistence
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.ShardStatePersistenceFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.shouldBe
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import mu.KLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.junit.jupiter.Container
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.math.BigInteger
import java.time.Duration


internal abstract class AbstractKinesisAndRedisTest : AbstractRedisTest() {

    companion object : KLogging() {
        val CREDENTIALS_PROVIDER =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"))
        val REGION = Region.EU_WEST_1
        const val TEST_STREAM_NAME = "test-stream"
        const val TEST_APPLICATION_NAME = "test-application"

        @JvmStatic
        @Container
        var localStackContainer: LocalStackContainer = LocalStackContainer("0.10.8")
            .withServices(LocalStackContainer.Service.KINESIS)

        fun getKinesisEndpoint() =
            "http://${localStackContainer.containerIpAddress}:${localStackContainer.getMappedPort(LocalStackContainer.Service.KINESIS.port)}"
    }

    protected lateinit var kinesisClient: KinesisAsyncClient
    protected lateinit var shardStatePersistence: ShardStatePersistence

    @BeforeEach
    internal fun setUpSharedInstancesAndClients(vertx: Vertx) {
        shareCredentialsProvider(vertx)

        val shardStatePersistenceFactory = createAndShareShardStatePersistenceFactory(vertx)
        shardStatePersistence = shardStatePersistenceFactory.createShardStatePersistence(
            RedisAPI.api(
                Redis.createClient(
                    vertx,
                    redisOptions
                )
            )
        )

        kinesisClient = createAndShareKinesisAsyncClientFactory(vertx).createKinesisAsyncClient(context)
    }

    /**
     * Cleanup Kinesis streams after each test function as the Kinesis instance is per the class.
     */
    @AfterEach
    internal fun cleanupKinesisStreams(testContext: VertxTestContext) = asyncTest(testContext) {
        kinesisClient.listStreams().awaitSuspending().streamNames().forEach { streamName ->
            kinesisClient.deleteStream { builder ->
                builder.streamName(streamName)
            }.awaitSuspending()
        }

        // Stream deletion is delayed, so we have to poll but it's faster than to restart the whole localstack
        var streamsAfterDeletion = kinesisClient.listStreams().awaitSuspending()
        while (streamsAfterDeletion.streamNames().isNotEmpty()) {
            streamsAfterDeletion = kinesisClient.listStreams().awaitSuspending()
        }
        logger.info { "Kinesis streams deleted" }
    }

    private fun shareCredentialsProvider(vertx: Vertx) {
        SharedData.shareInstance(
            vertx,
            ShareableAwsCredentialsProvider(CREDENTIALS_PROVIDER),
            ShareableAwsCredentialsProvider.SHARED_DATA_REF
        )
    }

    private fun createAndShareShardStatePersistenceFactory(vertx: Vertx): ShardStatePersistenceFactory {
        return createShardStatePersistenceFactory().also {
            SharedData.shareInstance(vertx, it, ShardStatePersistenceFactory.SHARED_DATA_REF)
        }
    }

    private fun createAndShareKinesisAsyncClientFactory(vertx: Vertx): KinesisAsyncClientFactory {
        val kinesisAsyncClientFactory = KinesisAsyncClientFactory(vertx, REGION.id(), getKinesisEndpoint())
        SharedData.shareInstance(vertx, kinesisAsyncClientFactory, KinesisAsyncClientFactory.SHARED_DATA_REF)
        return kinesisAsyncClientFactory
    }

    private fun createLocalstackKinesisClient(
        context: Context,
        region: Region
    ): KinesisAsyncClient = VertxSdkClient.withVertx(
        KinesisAsyncClient.builder().region(
            region
        ).endpointOverride(
            getKinesisEndpoint()
                .toUri()
        ).credentialsProvider(CREDENTIALS_PROVIDER), context
    ).build()

    private fun createShardStatePersistenceFactory(): ShardStatePersistenceFactory {
        return ShardStatePersistenceFactory(
            Duration.ofMillis(VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION),
            RedisKeyFactory(TEST_APPLICATION_NAME, TEST_STREAM_NAME)
        )
    }

    protected suspend fun putRecords(
        recordBundleCount: Int,
        recordCountPerBundle: Int,
        recordDataSupplier: (Int) -> SdkBytes = { count -> SdkBytes.fromUtf8String("record-data-$count") },
        partitionKeySupplier: (Int) -> String = { "partition-key_$it" }
    ) {
        repeat(recordBundleCount) { bundleIdx ->
            // Partition key is per bundle
            val partitionKey = partitionKeySupplier(bundleIdx)
            val putRequestRecords = Array<PutRecordsRequestEntry>(recordCountPerBundle) { recordIdx ->
                PutRecordsRequestEntry.builder().partitionKey(partitionKey).data(recordDataSupplier(recordIdx))
                    .build()
            }.toList()
            val putResponse = kinesisClient.putRecords {
                it.records(putRequestRecords).streamName(TEST_STREAM_NAME)
            }.awaitSuspending()
            logger.info { "Did put ${putRequestRecords.size} records" }
            putResponse.failedRecordCount().shouldBe(0)
        }
    }

    protected suspend fun putRecordsExplicitHashKey(
        recordBundleCount: Int,
        recordCountPerBundle: Int,
        recordDataSupplier: (Int) -> SdkBytes = { count -> SdkBytes.fromUtf8String("record-data-$count") },
        streamName: String = TEST_STREAM_NAME,
        predefinedShards: ShardList? = null
    ) {
        // Count of record bundles must equal to the count of shards
        val shards = predefinedShards ?: kinesisClient.streamDescriptionWhenActiveAwait(streamName).shards()
        shards.shouldHaveSize(recordBundleCount)

        repeat(recordBundleCount) { bundleIdx ->
            val hashKey = shards[bundleIdx].hashKeyRange().startingHashKey()
            val putRequestRecords = Array<PutRecordsRequestEntry>(recordCountPerBundle) { recordIdx ->
                PutRecordsRequestEntry.builder().explicitHashKey(hashKey).partitionKey("partition-key")
                    .data(recordDataSupplier(recordIdx))
                    .build()
            }.toList()
            val putResponse = kinesisClient.putRecords {
                it.records(putRequestRecords).streamName(TEST_STREAM_NAME)
            }.awaitSuspending()
            logger.info { "Did put ${putRequestRecords.size} records" }
            putResponse.failedRecordCount().shouldBe(0)
        }
    }

    protected suspend fun createAndGetStreamDescriptionWhenActive(shardCount: Int): StreamDescription {
        kinesisClient.createStream {
            it.streamName(TEST_STREAM_NAME).shardCount(shardCount)
        }.awaitSuspending()
        return kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME).also {
            logger.info { "Test Kinesis stream \"$TEST_STREAM_NAME\" with \"$shardCount\" shards created" }
        }
    }

    protected suspend fun splitShardFair(shardToSplit: Shard) {
        // https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding-split.html
        kinesisClient.splitShard {
            val startingHashKey = BigInteger(shardToSplit.hashKeyRange().startingHashKey())
            val endingHashKey = BigInteger(shardToSplit.hashKeyRange().endingHashKey())
            val newStartingHashKey = startingHashKey.add(endingHashKey).divide(BigInteger("2")).toString()
            it.streamName(TEST_STREAM_NAME).shardToSplit(shardToSplit.shardId())
                .newStartingHashKey(newStartingHashKey)
        }.awaitSuspending()
    }

    protected suspend fun mergeShards(parentShard: Shard, adjacentShard: Shard) {
        // https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding-merge.html
        kinesisClient.mergeShards {
            it.streamName(TEST_STREAM_NAME)
            it.shardToMerge(parentShard.shardId())
            it.adjacentShardToMerge(adjacentShard.shardId())
        }.awaitSuspending()
    }
}
