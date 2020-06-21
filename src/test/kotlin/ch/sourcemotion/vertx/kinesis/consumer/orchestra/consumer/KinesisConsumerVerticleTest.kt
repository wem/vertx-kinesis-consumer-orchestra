package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle.Companion.CONSUMER_START_CMD_ADDR
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.SingleRecordForwardConsumerTestVerticle.Companion.SINGLE_RECORD_SEND_ADDR
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.LocalCodec
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReOrchestrationCmdDispatcher
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.bunchesOf
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
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
import java.util.concurrent.TimeUnit

internal class KinesisConsumerVerticleTest : AbstractKinesisAndRedisTest() {

    private var consumerDeploymentId: String? = null

    @BeforeEach
    internal fun setUpRecordMessageCodec() {
        eventBus.registerDefaultCodec(Record::class.java, LocalCodec("record-test-codec"))
    }

    @Test
    internal fun single_shard_consumer_sunny_case(vertx: Vertx, testContext: VertxTestContext) {
        val recordBunching = 10 bunchesOf 10

        asyncTestDelayedEnd(testContext, recordBunching.recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                msg.ack()
                checkpoint.flag()
            }

            deploySingleRecordForwardConsumerTestVerticle()
            startConsumerVerticles(streamDescription)

            putRecords(recordBunching)
        }
    }

    @Test
    internal fun consumer_retry_on_failure_by_configuration(vertx: Vertx, testContext: VertxTestContext) {
        val recordBunching =
            10 bunchesOf 10 addToCount 11 // + 11 because each 10th record processing will fail and retried one time.

        asyncTestDelayedEnd(testContext, recordBunching.recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var receivedRecords = 0
            var lastFailedRecordSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                val record = msg.body()

                if (++receivedRecords % recordBunching.recordsPerBunch == 0) {
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

            deploySingleRecordForwardConsumerTestVerticle()
            startConsumerVerticles(streamDescription)

            putRecords(recordBunching)
        }
    }

    @Test
    internal fun consumer_retry_on_failure_by_exception_override(vertx: Vertx, testContext: VertxTestContext) {
        val recordBunching =
            10 bunchesOf 10 addToCount 11 // + 11 because each 10th record processing will fail and retried one time.

        asyncTestDelayedEnd(testContext, recordBunching.recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var receivedRecords = 0
            var lastFailedRecordSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                val record = msg.body()

                if (++receivedRecords % recordBunching.recordsPerBunch == 0) {
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

            deploySingleRecordForwardConsumerTestVerticle(
                createKinesisConsumerVerticleConfig(errorHandling = ErrorHandling.IGNORE_AND_CONTINUE)
            )
            startConsumerVerticles(streamDescription)
            putRecords(recordBunching)
        }
    }

    @Test
    internal fun consumer_ignore_on_failure_by_configuration(vertx: Vertx, testContext: VertxTestContext) {
        val recordBunching = 10 bunchesOf 10

        asyncTestDelayedEnd(testContext, recordBunching.recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            val recordSequenceNumbers = mutableListOf<String>()

            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                val record = msg.body()
                val sequenceNumber = record.sequenceNumber()
                testContext.verify { recordSequenceNumbers.shouldNotContain(sequenceNumber) }
                recordSequenceNumbers.add(sequenceNumber)

                // Each record processing will fail
                msg.fail(0, "")

                checkpoint.flag()
            }.completionHandlerAwait()

            deploySingleRecordForwardConsumerTestVerticle(
                createKinesisConsumerVerticleConfig(
                    errorHandling = ErrorHandling.IGNORE_AND_CONTINUE,
                    recordsPerPoll = 1,
                    pollIntervalMillis = 10
                )
            )
            startConsumerVerticles(streamDescription)

            putRecords(recordBunching)
        }
    }

    /**
     * Test, that the exception handling behavior defined by exception overrides the original configuration.
     */
    @Test
    internal fun consumer_ignore_on_failure_exception_override(vertx: Vertx, testContext: VertxTestContext) {
        val recordBunching = 10 bunchesOf 10

        asyncTestDelayedEnd(testContext, recordBunching.recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var lastReceivedRecordSequenceNumber: String? = null

            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                // Every time the same / first record is excepted
                val record = msg.body()
                if (lastReceivedRecordSequenceNumber.isNotNullOrBlank()) {
                    testContext.verify { lastReceivedRecordSequenceNumber.shouldNotBe(record.sequenceNumber()) }
                    lastReceivedRecordSequenceNumber = record.sequenceNumber()
                }

                msg.replyConsumeRecordFailedIgnore(record)

                checkpoint.flag()
            }.completionHandlerAwait()

            deploySingleRecordForwardConsumerTestVerticle(
                createKinesisConsumerVerticleConfig(
                    errorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
                    recordsPerPoll = 1,
                    pollIntervalMillis = 10
                )
            )
            startConsumerVerticles(streamDescription)

            putRecords(recordBunching)
        }
    }

    /**
     * If the orchestra is configured to retry from failed record, but a failure (Exception) not contains the necessary
     * information (record), the orchestra will retry beginning from the previous iterator.
     */
    @Test
    internal fun consumer_retry_from_failed_configured_but_wrong_exception(
        vertx: Vertx,
        testContext: VertxTestContext
    ) {
        val recordBunching = 10 bunchesOf 10

        asyncTest(testContext, recordBunching.recordCount * 2 /*In fact this is an endless loop*/) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var recordSequenceNumber: String? = null

            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                // Every time the same / first record is excepted
                val record = msg.body()
                if (recordSequenceNumber.isNotNullOrBlank()) {
                    testContext.verify { recordSequenceNumber.shouldBe(record.sequenceNumber()) }
                } else {
                    recordSequenceNumber = record.sequenceNumber()
                }

                // Each record processing will fail
                msg.fail(0, "")

                checkpoint.flag()
            }.completionHandlerAwait()

            deploySingleRecordForwardConsumerTestVerticle(
                createKinesisConsumerVerticleConfig(
                    errorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
                    pollIntervalMillis = 10
                )
            )
            startConsumerVerticles(streamDescription)

            putRecords(recordBunching)
        }
    }

    @Test
    internal fun shard_splitting(testContext: VertxTestContext) = asyncTestDelayedEnd(testContext, 1) { checkpoint ->
        val parentShardId = ShardIdGenerator.generateShardId()

        val streamDescription = createAndGetStreamDescriptionWhenActive(1)

        deploySingleRecordForwardConsumerTestVerticle(createKinesisConsumerVerticleConfig())
        startConsumerVerticles(streamDescription)

        ReOrchestrationCmdDispatcher.create(
            vertx,
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            kinesisClient,
            shardStatePersistence,
            defaultTestScope,
            redisOptions
        ) {
            defaultTestScope.launch {
                //Await stream is active again
                kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)

                val isParentShardInProgress = shardStatePersistence.isShardInProgress(parentShardId)
                val existingParentShardIterator = shardStatePersistence.getConsumerShardSequenceNumber(parentShardId)

                testContext.verify {
                    isParentShardInProgress.shouldBeFalse()
                    existingParentShardIterator.shouldBeNull()
                }
                checkpoint.flag()
            }
        }.start()

        splitShardFair(streamDescription.shards().first())
    }

    @Test
    internal fun shard_merging(testContext: VertxTestContext) = asyncTestDelayedEnd(testContext, 1) { checkpoint ->
        val shardIds = ShardIdGenerator.generateShardIdList(3)
        val parentShardId = shardIds.first()
        val adjacentParentShardId = shardIds[1]
        // We need 2 shard to merge
        val streamDescription = createAndGetStreamDescriptionWhenActive(2)

        deploySingleRecordForwardConsumerTestVerticle(
            createKinesisConsumerVerticleConfig(),
            streamDescription.shards().size
        )

        startConsumerVerticles(streamDescription)

        ReOrchestrationCmdDispatcher.create(
            vertx,
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            kinesisClient,
            shardStatePersistence,
            defaultTestScope,
            redisOptions
        ) {
            defaultTestScope.launch {
                val isParentShardInProgress = shardStatePersistence.isShardInProgress(parentShardId)
                val parentShardIterator = shardStatePersistence.getConsumerShardSequenceNumber(parentShardId)
                val isAdjacentParentShardInProgress = shardStatePersistence.isShardInProgress(adjacentParentShardId)
                val adjacentParentShardIterator =
                    shardStatePersistence.getConsumerShardSequenceNumber(adjacentParentShardId)

                val finishedShardIds = shardStatePersistence.getFinishedShardIds()

                testContext.verify {
                    isParentShardInProgress.shouldBeFalse()
                    parentShardIterator.shouldBeNull()
                    isAdjacentParentShardInProgress.shouldBeFalse()
                    adjacentParentShardIterator.shouldBeNull()

                    finishedShardIds.shouldContainAll(parentShardId, adjacentParentShardId)
                }
                checkpoint.flag()
            }
        }.start()

        mergeShards(
            streamDescription.shards().first { it.shardIdTyped() == parentShardId },
            streamDescription.shards().first { it.shardIdTyped() == adjacentParentShardId }
        )
    }

    /**
     * Simulates the restart of a consumer to test iterator persistence etc.
     */
    @Test
    internal fun consumer_restart_and_iterator_expiration(testContext: VertxTestContext) {
        val recordBunching = 1 bunchesOf 10
        val recordCount = recordBunching.recordCount

        asyncTestDelayedEnd(testContext, recordCount * 2/* We put the double amount of records*/) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            // We need a continued indexing of records, so we are able to identify each round the consumer will run.
            var putRecordIdx = 0
            val generateRecordData = { SdkBytes.fromUtf8String("$putRecordIdx").also { putRecordIdx += 1 } }

            val consumerRoundStarter = suspend {
                deploySingleRecordForwardConsumerTestVerticle()
                startConsumerVerticles(streamDescription)
            }

            val receivedRecordIndices = mutableListOf<Int>()

            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->
                msg.ack()
                val recordIdx = msg.body().data().asUtf8String().toInt()
                receivedRecordIndices.add(recordIdx)
                if (receivedRecordIndices.size == recordCount) {
                    logger.info { "Received first bunch of records. Restart consumer" }
                    defaultTestScope.launch {
                        testContext.verify { consumerDeploymentId.shouldNotBeNull() }
                        vertx.undeployAwait(consumerDeploymentId!!)

                        consumerRoundStarter()

                        putRecords(recordBunching, recordDataSupplier = { generateRecordData() })
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

            putRecords(recordBunching, recordDataSupplier = { generateRecordData() })
        }
    }

    @Disabled // Basically, the test is working, but there is a timeout problem. Will investigate later, there are some missing features ;)
    @Timeout(value = 310000, timeUnit = TimeUnit.MILLISECONDS)
    @Test
    internal fun shard_iterator_expiration(testContext: VertxTestContext) {
        val recordBunching = 1 bunchesOf 2

        asyncTestDelayedEnd(testContext, recordBunching.recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var receivedSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(SINGLE_RECORD_SEND_ADDR) { msg ->

                if (receivedSequenceNumber.isNotNull()) {
                    testContext.verify { receivedSequenceNumber.shouldNotBe(msg.body().sequenceNumber()) }
                } else {
                    receivedSequenceNumber = msg.body().sequenceNumber()

                    // Simulate very slow record processing, so iterator get expired
                    val consumerProcessingDuration = Duration.ofMinutes(5).plus(Duration.ofSeconds(2))

                    logger.info { "Simulate very slow consumer. Will take ${consumerProcessingDuration.toSeconds()} seconds to proceed." }
                    vertx.setTimer(consumerProcessingDuration.toMillis()) {
                        logger.info { "Very slow consumer processing done." }
                        msg.ack()
                    }
                }
                logger.info { "Record received" }
                checkpoint.flag()
            }.completionHandlerAwait()


            deploySingleRecordForwardConsumerTestVerticle(
                createKinesisConsumerVerticleConfig().copy(
                    recordsPerPollLimit = 1,
                    kinesisPollIntervalMillis = 10
                )
            )

            startConsumerVerticles(streamDescription)

            putRecords(recordBunching)
        }
    }


    @Timeout(value = 101, timeUnit = TimeUnit.SECONDS)
    @Test
    internal fun timeout(testContext: VertxTestContext) = asyncTest(testContext) {
        defaultTestScope.launch { delay(Duration.ofSeconds(100).toMillis()) }
    }

    /**
     * Start consumer verticle like orchestra verticle will do.
     */
    private suspend fun startConsumerVerticles(streamDescription: StreamDescription) {
        streamDescription.shards().forEach { shard ->
            vertx.eventBus()
                .requestAwait<Unit>(CONSUMER_START_CMD_ADDR, shard.shardId(), DeliveryOptions().setLocalOnly(true))
        }
    }

    private suspend fun deploySingleRecordForwardConsumerTestVerticle(
        options: KinesisConsumerVerticleOptions = createKinesisConsumerVerticleConfig(),
        instances: Int = 1
    ) {
        deployVerticle(options, SingleRecordForwardConsumerTestVerticle::class.java.name, instances)
    }

    private suspend fun deployVerticle(
        options: KinesisConsumerVerticleOptions,
        verticleClass: String,
        instances: Int = 1
    ) {
        consumerDeploymentId = vertx.deployVerticleAwait(
            verticleClass,
            DeploymentOptions().setConfig(JsonObject.mapFrom(options)).setInstances(instances)
        )
    }

    private fun createKinesisConsumerVerticleConfig(
        applicationName: String = TEST_APPLICATION_NAME,
        streamName: String = TEST_STREAM_NAME,
        shardIteratorStrategy: ShardIteratorStrategy = ShardIteratorStrategy.EXISTING_OR_LATEST,
        errorHandling: ErrorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
        pollIntervalMillis: Long = 1000L,
        recordsPerPoll: Int = 1000
    ) = KinesisConsumerVerticleOptions(
        applicationName,
        streamName,
        shardIteratorStrategy,
        errorHandling,
        pollIntervalMillis,
        recordsPerPoll,
        redisOptions
    )
}

class SingleRecordForwardConsumerTestVerticle : AbstractKinesisConsumerCoroutineVerticle() {

    companion object {
        const val SINGLE_RECORD_SEND_ADDR = "/kinesis-consumer-orchester/testing/single-record"
    }

    override suspend fun onRecordsAsync(records: List<Record>) {
        runCatching {
            records.forEach { record ->
                vertx.eventBus().requestAwait<Unit>(
                    SINGLE_RECORD_SEND_ADDR,
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
