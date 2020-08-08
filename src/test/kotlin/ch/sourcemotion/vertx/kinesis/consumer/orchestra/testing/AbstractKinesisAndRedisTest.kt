package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardList
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.deploymentOptionsOf
import kotlinx.coroutines.future.await
import mu.KLogging
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


internal abstract class AbstractKinesisAndRedisTest(private val deployShardPersistence: Boolean = true) : AbstractRedisTest() {

    companion object : KLogging() {
        val CREDENTIALS_PROVIDER: StaticCredentialsProvider =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"))
        val REGION: Region = Region.EU_WEST_1
        const val TEST_STREAM_NAME = "test-stream"
        const val TEST_APPLICATION_NAME = "test-application"

        @JvmStatic
        @Container
        var localStackContainer: LocalStackContainer = LocalStackContainer("0.10.8")
            .withServices(LocalStackContainer.Service.KINESIS)

        fun getKinesisEndpoint() =
            "http://${localStackContainer.containerIpAddress}:${localStackContainer.getMappedPort(LocalStackContainer.Service.KINESIS.port)}"
    }

    protected val kinesisClient: KinesisAsyncClient by lazy {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    protected val shardStatePersistenceService: ShardStatePersistenceServiceAsync by lazy {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    @BeforeEach
    fun credentialsProviderKinesisClientFactoryAndShardPersistence(testContext: VertxTestContext) =
        asyncTest(testContext) {
            shareCredentialsProvider()
            createAndShareKinesisAsyncClientFactory().createKinesisAsyncClient(context)
            if (deployShardPersistence) {
                deployShardStatePersistenceService()
            }
        }


    /**
     * Cleanup Kinesis streams before each test function as the Kinesis instance is per the class.
     */
    @BeforeEach
    fun cleanupKinesisStreams(testContext: VertxTestContext) = asyncTest(testContext) {
        val streamNames = kinesisClient.listStreams().await().streamNames()
        if (streamNames.isNotEmpty()) {
            streamNames.forEach { streamName ->
                kinesisClient.deleteStream { builder ->
                    builder.streamName(streamName)
                }.await()
            }

            // Stream deletion is delayed, so we have to poll but it's faster than to restart the whole localstack
            var streamsAfterDeletion = kinesisClient.listStreams().await()
            while (streamsAfterDeletion.streamNames().isNotEmpty()) {
                streamsAfterDeletion = kinesisClient.listStreams().await()
            }
            logger.info { "Kinesis streams cleaned up" }
        } else {
            logger.info { "Kinesis stream clean up not necessary" }
        }
    }

    private fun shareCredentialsProvider() {
        SharedData.shareInstance(
            vertx,
            ShareableAwsCredentialsProvider(CREDENTIALS_PROVIDER),
            ShareableAwsCredentialsProvider.SHARED_DATA_REF
        )
    }

    private suspend fun deployShardStatePersistenceService() {
        val options = RedisShardStatePersistenceServiceVerticleOptions(
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            redisOptions,
            VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS
        )
        vertx.deployVerticleAwait(
            RedisShardStatePersistenceServiceVerticle::class.java.name, deploymentOptionsOf(JsonObject.mapFrom(options))
        )
    }

    private fun createAndShareKinesisAsyncClientFactory(): KinesisAsyncClientFactory {
        val kinesisAsyncClientFactory = KinesisAsyncClientFactory(vertx, REGION.id(), getKinesisEndpoint())
        SharedData.shareInstance(vertx, kinesisAsyncClientFactory, KinesisAsyncClientFactory.SHARED_DATA_REF)
        return kinesisAsyncClientFactory
    }

    protected suspend fun putRecords(
        recordBunching: RecordPutBunching,
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
            val putResponse = kinesisClient.putRecords {
                it.records(putRequestRecords).streamName(TEST_STREAM_NAME)
            }.await()
            logger.info { "Did put ${putRequestRecords.size} records" }
            putResponse.failedRecordCount().shouldBe(0)
        }
    }

    protected suspend fun putRecordsExplicitHashKey(
        recordBunching: RecordPutBunching,
        recordDataSupplier: (Int) -> SdkBytes = { count -> SdkBytes.fromUtf8String("record-data-$count") },
        streamName: String = TEST_STREAM_NAME,
        predefinedShards: ShardList? = null
    ) {
        // Count of record bundles must equal to the count of shards
        val shards = predefinedShards ?: kinesisClient.streamDescriptionWhenActiveAwait(streamName).shards()
        shards.shouldHaveSize(recordBunching.recordBunches)

        repeat(recordBunching.recordBunches) { bundleIdx ->
            val hashKey = shards[bundleIdx].hashKeyRange().startingHashKey()

            val putRequestRecords = List(recordBunching.recordsPerBunch) { recordIdx ->
                PutRecordsRequestEntry.builder().explicitHashKey(hashKey).partitionKey("partition-key")
                    .data(recordDataSupplier(recordIdx))
                    .build()
            }.toList()
            val putResponse = kinesisClient.putRecords {
                it.records(putRequestRecords).streamName(TEST_STREAM_NAME)
            }.await()
            logger.info { "Did put ${putRequestRecords.size} records" }
            putResponse.failedRecordCount().shouldBe(0)
        }
    }

    protected suspend fun createAndGetStreamDescriptionWhenActive(shardCount: Int): StreamDescription {
        kinesisClient.createStream {
            it.streamName(TEST_STREAM_NAME).shardCount(shardCount)
        }.await()
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
        }.await()
    }

    protected suspend fun mergeShards(parentShard: Shard, adjacentShard: Shard) {
        // https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding-merge.html
        kinesisClient.mergeShards {
            it.streamName(TEST_STREAM_NAME)
            it.shardToMerge(parentShard.shardId())
            it.adjacentShardToMerge(adjacentShard.shardId())
        }.await()
    }
}
