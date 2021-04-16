package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.EventBusAddr
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.LocalCodec
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.MergeReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingType
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.SplitReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.collections.*
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.ReplyException
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.undeployAwait
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.time.Duration

internal abstract class AbstractKinesisConsumerVerticleTest : AbstractKinesisAndRedisTest() {
    @BeforeEach
    internal fun setUpRecordMessageCodec() {
        eventBus.registerDefaultCodec(Record::class.java, LocalCodec("record-test-codec"))
    }

    @Test
    internal fun single_shard_consumer_sunny_case(vertx: Vertx, testContext: VertxTestContext) {
        val recordBatching = 10 batchesOf 10

        testContext.async(recordBatching.recordCount) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            var records = 0

            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                msg.ack()
                println("record ${++records}")
                checkpoint.flag()
            }.completionHandlerAwait()

            deployTestConsumerVerticle(
                createKinesisConsumerVerticleConfig(
                    streamDescription,
                    streamDescription.getFirstShardId()
                )
            )

            delay(1000)
            kinesisClient.putRecords(recordBatching)
        }
    }

    @Disabled("https://github.com/localstack/localstack/issues/3823")
    @Test
    internal fun consumer_retry_on_failure_by_configuration(vertx: Vertx, testContext: VertxTestContext) {
        val recordBatching =
            10 batchesOf 10 addToCount 11 // + 11 because each 10th record processing will fail and retried one time.

        testContext.asyncDelayed(recordBatching.recordCount) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            var receivedRecords = 0
            var lastFailedRecordSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                val record = msg.body()

                if (++receivedRecords % recordBatching.recordsPerBatch == 0) {
                    lastFailedRecordSequenceNumber = record.sequenceNumber()
                    msg.replyConsumeRecordFailedRetry(record)
                } else {
                    // If the last failed sequence number is present, it's expected that the next record is the same here, as retry is configured
                    if (lastFailedRecordSequenceNumber.isNotNullOrBlank()) {
                        testContext.verify { lastFailedRecordSequenceNumber.shouldBe(record.sequenceNumber()) }
                        lastFailedRecordSequenceNumber = null
                    }
                    msg.ack()
                }
                logger.info { "Received $receivedRecords records" }
                checkpoint.flag()
            }.completionHandlerAwait()

            deployTestConsumerVerticle(
                createKinesisConsumerVerticleConfig(
                    streamDescription,
                    streamDescription.getFirstShardId()
                )
            )

            kinesisClient.putRecords(recordBatching)
        }
    }

    @Disabled("https://github.com/localstack/localstack/issues/3823")
    @Test
    internal fun consumer_retry_on_failure_by_exception_override(vertx: Vertx, testContext: VertxTestContext) {
        val recordBatching =
            10 batchesOf 10 addToCount 11 // + 11 because each 10th record processing will fail and retried one time.

        testContext.asyncDelayed(recordBatching.recordCount) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            var receivedRecords = 0
            var lastFailedRecordSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                val record = msg.body()

                if (++receivedRecords % recordBatching.recordsPerBatch == 0) {
                    lastFailedRecordSequenceNumber = record.sequenceNumber()
                    msg.replyConsumeRecordFailedRetry(record)
                } else {
                    // If the last failed sequence number is present, it's expected that the next record is the same here, as retry is configured
                    if (lastFailedRecordSequenceNumber.isNotNullOrBlank()) {
                        testContext.verify { lastFailedRecordSequenceNumber.shouldBe(record.sequenceNumber()) }
                        lastFailedRecordSequenceNumber = null
                    }
                    msg.ack()
                }
                checkpoint.flag()
            }.completionHandlerAwait()

            deployTestConsumerVerticle(
                createKinesisConsumerVerticleConfig(
                    streamDescription,
                    streamDescription.getFirstShardId(),
                    errorHandling = ErrorHandling.IGNORE_AND_CONTINUE
                )
            )
            kinesisClient.putRecords(recordBatching)
        }
    }

    @Test
    internal fun consumer_ignore_on_failure_by_configuration(vertx: Vertx, testContext: VertxTestContext) {
        testContext.async(10) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            val recordSequenceNumbers = mutableListOf<String>()

            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                val record = msg.body()
                val sequenceNumber = record.sequenceNumber()
                testContext.verify { recordSequenceNumbers.shouldNotContain(sequenceNumber) }
                recordSequenceNumbers.add(sequenceNumber)

                // Each record processing will fail
                msg.fail(0, "")

                defaultTestScope.launch {
                    kinesisClient.putRecords(1 batchesOf 1)
                }

                checkpoint.flag()
            }.completionHandlerAwait()

            deployTestConsumerVerticle(
                createKinesisConsumerVerticleConfig(
                    streamDescription,
                    streamDescription.getFirstShardId(),
                    errorHandling = ErrorHandling.IGNORE_AND_CONTINUE,
                )
            )

            delay(1000)
            kinesisClient.putRecords(1 batchesOf 1)
        }
    }

    /**
     * Test, that the exception handling behavior defined by exception overrides the original configuration.
     */
    @Test
    internal fun consumer_ignore_on_failure_exception_override(testContext: VertxTestContext) {
        testContext.async(10) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            val recordSequenceNumbers = mutableListOf<String>()

            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                val record = msg.body()
                val sequenceNumber = record.sequenceNumber()
                testContext.verify { recordSequenceNumbers.shouldNotContain(sequenceNumber) }
                recordSequenceNumbers.add(sequenceNumber)

                msg.replyConsumeRecordFailedIgnore(record)

                defaultTestScope.launch {
                    kinesisClient.putRecords(1 batchesOf 1)
                }

                checkpoint.flag()
            }.completionHandlerAwait()

            deployTestConsumerVerticle(
                createKinesisConsumerVerticleConfig(
                    streamDescription,
                    streamDescription.getFirstShardId(),
                    errorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
                )
            )

            delay(1000)
            kinesisClient.putRecords(1 batchesOf 1)
        }
    }

    /**
     * If the orchestra is configured to retry from failed record, but a failure (Exception) not contains the necessary
     * information (record), the orchestra will retry beginning from the previous iterator.
     */
    @Disabled("https://github.com/localstack/localstack/issues/3823")
    @Test
    internal fun consumer_retry_from_failed_configured_but_wrong_exception(
        vertx: Vertx,
        testContext: VertxTestContext
    ) {
        val recordBatching = 1 batchesOf 10

        testContext.async(recordBatching.recordCount) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            var recordSequenceNumber: String? = null

            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                // Every time the same / first record is excepted
                val record = msg.body()
                if (recordSequenceNumber.isNotNullOrBlank()) {
                    testContext.verify { recordSequenceNumber.shouldBe(record.sequenceNumber()) }
                } else {
                    recordSequenceNumber = record.sequenceNumber()
                }

                // Each record processing will fail, but
                msg.fail(0, "Test failure")

                checkpoint.flag()
            }.completionHandlerAwait()

            deployTestConsumerVerticle(
                createKinesisConsumerVerticleConfig(
                    streamDescription,
                    streamDescription.getFirstShardId(),
                    errorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
                )
            )

            kinesisClient.putRecords(recordBatching)
        }
    }

    @Test
    internal fun split_resharding(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val (parentShardId, firstChild, secondChild) = ShardIdGenerator.reshardingIdConstellation()
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

        deployTestConsumerVerticle(createKinesisConsumerVerticleConfig(streamDescription, parentShardId))

        var recordsReceived = 0

        eventBus.consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
            if (++recordsReceived == 100) {
                defaultTestScope.launch {
                    kinesisClient.splitShardFair(streamDescription.shards().first())
                }
            }
            msg.ack()
        }.completionHandlerAwait()

        eventBus.consumer<ReshardingEvent>(EventBusAddr.resharding.notification) { msg ->
            val event = msg.body()
            testContext.verify {
                event.reshardingType.shouldBe(ReshardingType.SPLIT)
                event.shouldBeInstanceOf<SplitReshardingEvent>()
                event.finishedParentShardId.shouldBe(parentShardId)
                event.childShardIds.shouldContainExactlyInAnyOrder(firstChild, secondChild)
            }
            checkpoint.flag()
        }.completionHandlerAwait()

        vertx.setPeriodic(10) {
            defaultTestScope.launch {
                kinesisClient.putRecords(1 batchesOf 1)
            }
        }
    }

    @Test
    internal fun merge_resharding(testContext: VertxTestContext) = testContext.async(2) { checkpoint ->
        val (parentShardId, adjacentParentShardId, childShardId) = ShardIdGenerator.reshardingIdConstellation()
        val parentShardIds = listOf(parentShardId, adjacentParentShardId).toMutableList()

        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)

        streamDescription.shards().forEach { shard ->
            deployTestConsumerVerticle(createKinesisConsumerVerticleConfig(streamDescription, shard.shardIdTyped()))
        }

        var resharded = false
        var recordsReceived = 0

        eventBus.consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
            if (resharded.not() && ++recordsReceived == 100) {
                resharded = true
                defaultTestScope.launch {
                    kinesisClient.mergeShards(streamDescription.shards())
                }
            }
            msg.ack()
        }.completionHandlerAwait()

        eventBus.consumer<ReshardingEvent>(EventBusAddr.resharding.notification) { msg ->
            val event = msg.body()
            testContext.verify {
                event.reshardingType.shouldBe(ReshardingType.MERGE)
                event.shouldBeInstanceOf<MergeReshardingEvent>()

                val finishedParentShardId = event.finishedParentShardId
                parentShardIds.shouldContain(finishedParentShardId)
                parentShardIds.remove(finishedParentShardId)
                event.childShardId.shouldBe(childShardId)
            }
            checkpoint.flag()
        }.completionHandlerAwait()

        vertx.setPeriodic(10) {
            defaultTestScope.launch {
                kinesisClient.putRecordsExplicitHashKey(2 batchesOf 1, predefinedShards = streamDescription.shards())
            }
        }
    }

    /**
     * Simulates the restart of a consumer to test interaction with iterator persistence etc.
     */
    @Test
    internal fun consumer_restart(testContext: VertxTestContext) {
        val recordBatching = 1 batchesOf 10
        val recordCount = recordBatching.recordCount

        testContext.asyncDelayed(recordCount * 2/* We put the double amount of records*/) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            // We need a continued indexing of records, so we are able to identify each round the consumer will run.
            var putRecordIdx = 0
            val generateRecordData = { SdkBytes.fromUtf8String("${putRecordIdx++}") }

            var consumerDeploymentId: String? = null

            val consumerRoundStarter = suspend {
                consumerDeploymentId =
                    deployTestConsumerVerticle(
                        createKinesisConsumerVerticleConfig(
                            streamDescription,
                            streamDescription.getFirstShardId()
                        )
                    )
            }

            val receivedRecordIndices = mutableListOf<Int>()

            vertx.eventBus().consumer<Record>(TestConsumerVerticle.RECORD_SEND_ADDR) { msg ->
                val recordIdx = msg.body().data().asUtf8String().toInt()
                if (receivedRecordIndices.isNotEmpty()) {
                    recordIdx.shouldBe(receivedRecordIndices.last() + 1)
                }
                receivedRecordIndices.add(recordIdx)

                msg.ack()
                if (receivedRecordIndices.size == recordCount) {
                    logger.info { "Received first bunch of records. Restart consumer" }
                    defaultTestScope.launch {
                        testContext.verify { consumerDeploymentId.shouldNotBeNull() }
                        vertx.undeployAwait(consumerDeploymentId!!)

                        consumerRoundStarter()

                        delay(1000)
                        kinesisClient.putRecords(recordBatching, recordDataSupplier = { generateRecordData() })
                    }
                }
                // Verification of the second run / deployment of the consumer verticle.
                // So we test iterator persistence works as expected
                if (receivedRecordIndices.size == recordCount * 2) {
                    testContext.verify {
                        repeat(recordCount) { idx ->
                            val expectedIdx = idx + recordCount
                            receivedRecordIndices.shouldContain(expectedIdx)
                        }
                    }
                }
                checkpoint.flag()
            }.completionHandlerAwait()

            consumerRoundStarter()

            delay(1000)
            kinesisClient.putRecords(recordBatching, recordDataSupplier = { generateRecordData() })
        }
    }

    @Test
    internal fun shard_progress_flagging(testContext: VertxTestContext) = testContext.async {
        val shardProgressExpirationMillis = 100L
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val shardId = streamDescription.getFirstShardId()
        val deploymentId = deployTestConsumerVerticle(
            createKinesisConsumerVerticleConfig(
                streamDescription,
                shardId,
                shardProgressExpirationMillis = shardProgressExpirationMillis
            )
        )
        shardStatePersistenceService.getShardIdsInProgress().shouldContainExactly(shardId)
        delay(shardProgressExpirationMillis * 2)
        shardStatePersistenceService.getShardIdsInProgress().shouldContainExactly(shardId)
        vertx.undeployAwait(deploymentId)
        shardStatePersistenceService.getShardIdsInProgress().shouldBeEmpty()
    }

    private fun StreamDescription.getFirstShardId() = shards().first().shardIdTyped()

    private suspend fun deployTestConsumerVerticle(
        options: KinesisConsumerVerticleOptions,
        instances: Int = 1
    ) = deployTestVerticle<TestConsumerVerticle>(options, instances)

    private fun createKinesisConsumerVerticleConfig(
        streamDescription: StreamDescription,
        shardId: ShardId,
        shardIteratorStrategy: ShardIteratorStrategy = ShardIteratorStrategy.EXISTING_OR_LATEST,
        errorHandling: ErrorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
        shardProgressExpirationMillis: Long = VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS,
    ) = KinesisConsumerVerticleOptions(
        shardId,
        OrchestraClusterName(TEST_APPLICATION_NAME, TEST_STREAM_NAME),
        shardIteratorStrategy,
        errorHandling,
        shardProgressExpirationMillis = shardProgressExpirationMillis,
        fetcherOptions = fetcherOptions(streamDescription)
    )

    protected abstract fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions

}

class TestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {

    companion object {
        const val RECORD_SEND_ADDR = "/kinesis-consumer-orchester/testing/single-record"
    }

    override suspend fun onRecordsAsync(records: List<Record>) {
        runCatching {
            records.forEach { record ->
                vertx.eventBus().requestAwait<Unit>(
                    RECORD_SEND_ADDR,
                    record,
                    // Ensure send will not timeout, even on longer running tests
                    DeliveryOptions().setSendTimeout(Duration.ofMinutes(10).toMillis())
                )
            }
        }.exceptionOrNull()?.let { throwable ->
            throw if (throwable is ReplyException && throwable.isKinesisConsumerException()) {
                throwable.toKinesisConsumerException()
            } else {
                throwable
            }
        }
    }
}