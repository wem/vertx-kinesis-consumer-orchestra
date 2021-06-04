package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.getShardIteratorAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType

internal class ComponentWithImportTest : AbstractKinesisAndRedisTest() {

    companion object {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        private const val RECORD_COUNT_AFTER_KCLV1_IMPORT = 100
    }

    private val dynamoDbClient by lazy {
        val builder = DynamoDbAsyncClient.builder().apply {
            credentialsProvider(AWS_CREDENTIALS_PROVIDER)
        }
        VertxSdkClient.withVertx(builder, context).build()
    }

    private var orchestra: VertxKinesisOrchestra? = null

    @BeforeEach
    internal fun setUpComponent() = asyncBeforeOrAfter {
        dynamoDbClient.forceCreateLeaseTable(LEASE_TABLE_NAME)
    }

    @AfterEach
    internal fun tearDown() = asyncBeforeOrAfter {
        dynamoDbClient.deleteTableIfExists(LEASE_TABLE_NAME)
    }

    @AfterEach
    internal fun closeOrchestra() = asyncBeforeOrAfter {
        orchestra?.closeAwait()
    }

    @Test
    internal fun consume_some_records(testContext: VertxTestContext) =
        asyncTest(testContext, RECORD_COUNT_AFTER_KCLV1_IMPORT) { checkpoint ->
            val shardId = kinesisClient.createAndGetStreamDescriptionWhenActive(1).shards().first().shardIdTyped()

            val shardIterator = kinesisClient.getShardIteratorAwait(TEST_STREAM_NAME, ShardIteratorType.LATEST, shardId)

            // Put records onto the stream, so we can simulate KCLv1 processing before VKCO starts
            kinesisClient.putRecords(1 batchesOf 1)

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
                    credentialsProviderSupplier = { AWS_CREDENTIALS_PROVIDER },
                    consumerVerticleClass = ComponentWithImportTestConsumerVerticle::class.java.name,
                    redisOptions = redisHeimdallOptions,
                    consumerVerticleOptions = JsonObject.mapFrom(ComponentTestConsumerOptions(AbstractComponentTest.PARAMETER_VALUE)),
                    kclV1ImportOptions = KCLV1ImportOptions(leaseTableName = LEASE_TABLE_NAME)
                )
            ).start().await()

            eventBus.consumer<JsonArray>(RECORD_FAN_OUT_ADDR) { msg ->
                msg.body().forEach { seqNbr ->
                    receivedRecordSequenceNumbers.contains(seqNbr).shouldBeFalse()
                    receivedRecordSequenceNumbers.add("$seqNbr")
                    checkpoint.flag()
                }
            }

            delay(10000)

            kinesisClient.putRecords(1 batchesOf RECORD_COUNT_AFTER_KCLV1_IMPORT)
        }
}


class ComponentWithImportTestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {
    override suspend fun onRecordsAsync(records: List<Record>) {
        vertx.eventBus().send(AbstractComponentTest.RECORD_FAN_OUT_ADDR, JsonArray(records.map { it.sequenceNumber() }))
    }
}


