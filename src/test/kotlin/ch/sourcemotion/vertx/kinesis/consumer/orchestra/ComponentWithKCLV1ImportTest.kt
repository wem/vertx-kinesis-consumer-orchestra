package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.getShardIteratorAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.future.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.junit.jupiter.Container
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import java.net.URI

internal class ComponentWithImportTest : AbstractRedisTest() {

    companion object {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val RECORD_COUNT = 100

        private const val LEASE_TABLE_NAME = "kcl_lease"
        private const val RECORD_COUNT_AFTER_KCLV1_IMPORT = 100

        @JvmStatic
        @Container
        var localStackContainer: LocalStackContainer = LocalStackContainer(Localstack.VERSION)
            .withServices(Service.DYNAMODB, Service.KINESIS)
    }

    private val dynamoDbClient by lazy {
        val builder = DynamoDbAsyncClient.builder().apply {
            endpointOverride(URI(localStackContainer.getDynamoDBEndpointOverride()))
            credentialsProvider(Localstack.credentialsProvider)
        }
        VertxSdkClient.withVertx(builder, context).build()
    }

    private val kinesisClient: KinesisAsyncClient by lazy {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    private var orchestra: VertxKinesisOrchestra? = null

    @BeforeEach
    internal fun setUpComponent(testContext: VertxTestContext) = asyncTest(testContext) {
        vertx.shareCredentialsProvider()
        vertx.shareKinesisAsyncClientFactory(localStackContainer.getKinesisEndpointOverride())
        dynamoDbClient.forceCreateLeaseTable(LEASE_TABLE_NAME)
    }

    @AfterEach
    internal fun closeOrchestra(testContext: VertxTestContext) = asyncTest(testContext) {
        orchestra?.closeAwait()
    }

    @Test
    internal fun consume_some_records(testContext: VertxTestContext) =
        asyncTest(testContext, RECORD_COUNT_AFTER_KCLV1_IMPORT) { checkpoint ->
            val shardId = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

            val shardIterator = kinesisClient.getShardIteratorAwait(TEST_STREAM_NAME, ShardIteratorType.LATEST, shardId)

            // Put records onto the stream, so we can simulate KCLv1 processing before VKCO starts
            kinesisClient.putRecords(1 bunchesOf 1)

            val receivedRecordSequenceNumbers = HashSet<String>()

            val getRecordsResponse = kinesisClient.getRecords { it.shardIterator(shardIterator.iter) }.await()
            val records = getRecordsResponse.records()
            records.shouldHaveSize(1)
            val record = records.first()
            receivedRecordSequenceNumbers.add(record.sequenceNumber())
            record.data().asUtf8String().shouldBe("record-data-0")

            // Put checkpoint (sequence number) into the lease table, so the VKCO can import it
            dynamoDbClient.putLeases(LEASE_TABLE_NAME, shardId to record.sequenceNumber())

            orchestra = VertxKinesisOrchestra.create(
                vertx, VertxKinesisOrchestraOptions(
                    TEST_APPLICATION_NAME,
                    TEST_STREAM_NAME,
                    credentialsProviderSupplier = { Localstack.credentialsProvider },
                    consumerVerticleClass = ComponentWithImportTestConsumerVerticle::class.java.name,
                    redisOptions = redisOptions,
                    consumerVerticleConfig = JsonObject.mapFrom(ComponentTestConsumerOptions(ComponentTest.PARAMETER_VALUE)),
                    kinesisEndpoint = localStackContainer.getKinesisEndpointOverride(),
                    kclV1ImportOptions = KCLV1ImportOptions(
                        leaseTableName = LEASE_TABLE_NAME,
                        dynamoDbEndpoint = localStackContainer.getDynamoDBEndpointOverride()
                    )
                )
            ).startAwait()


            eventBus.consumer<JsonArray>(RECORD_FAN_OUT_ADDR) { msg ->
                msg.body().forEach { seqNbr ->
                    receivedRecordSequenceNumbers.contains(seqNbr).shouldBeFalse()
                    receivedRecordSequenceNumbers.add("$seqNbr")
                    checkpoint.flag()
                }
            }

            kinesisClient.putRecords(1 bunchesOf RECORD_COUNT_AFTER_KCLV1_IMPORT)
        }
}


class ComponentWithImportTestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {
    override suspend fun onRecordsAsync(records: List<Record>) {
        vertx.eventBus().send(ComponentTest.RECORD_FAN_OUT_ADDR, JsonArray(records.map { it.sequenceNumber() }))
    }
}


