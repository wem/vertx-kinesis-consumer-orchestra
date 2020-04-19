package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle.Companion.CONSUMER_START_CMD_ADDR
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.LocalCodec
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.ReOrchestrationCmdDispatcher
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.eventbus.requestAwait
import kotlinx.coroutines.launch
import mu.KLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.StreamDescription

internal class KinesisConsumerVerticleTest : AbstractKinesisAndRedisTest() {

    companion object : KLogging() {
        const val RECORD_SEND_ADDR = "/kinesis-consumer-orchester/testing/record"
    }

    private var deploymentId: String? = null

    @BeforeEach
    internal fun setUpRecordMessageCodec() {
        eventBus.registerDefaultCodec(Record::class.java, LocalCodec("record-test-codec"))
    }

    @AfterEach
    internal fun undeployConsumerVerticle() {
        deploymentId?.let { vertx.undeploy(deploymentId) }
    }

    @Test
    internal fun single_shard_consumer_sunny_case(vertx: Vertx, testContext: VertxTestContext) {
        val recordBundleCount = 10
        val recordCountPerBundle = 10
        val recordCount = recordBundleCount * recordCountPerBundle

        asyncTest(testContext, recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            vertx.eventBus().consumer<Record>(RECORD_SEND_ADDR) { msg ->
                msg.ack()
                checkpoint.flag()
            }

            deploySingleRecordForwardConsumerTestVerticle(vertx)

            startConsumerVerticles(streamDescription)

            putRecords(recordBundleCount, recordCountPerBundle)
        }
    }

    @Test
    internal fun consumer_retry_on_failure_by_configuration(vertx: Vertx, testContext: VertxTestContext) {
        val recordBundles = 10
        val recordsPerBundle = 10
        val recordCount =
            recordBundles * recordsPerBundle + 11 // + 11 because each 10th record processing will fail and retried one time.

        asyncTest(testContext, recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var receivedRecords = 0
            var lastFailedRecordSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(RECORD_SEND_ADDR) { msg ->
                val record = msg.body()

                if (++receivedRecords % recordsPerBundle == 0) {
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
            }

            deploySingleRecordForwardConsumerTestVerticle(vertx)

            startConsumerVerticles(streamDescription)

            putRecords(recordBundles, recordsPerBundle)
        }
    }

    @Test
    internal fun consumer_retry_on_failure_by_exception_override(vertx: Vertx, testContext: VertxTestContext) {
        val recordBundles = 10
        val recordsPerBundle = 10
        val recordCount =
            recordBundles * recordsPerBundle + 11 // + 11 because each 10th record processing will fail and retried one time.

        asyncTest(testContext, recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var receivedRecords = 0
            var lastFailedRecordSequenceNumber: String? = null
            vertx.eventBus().consumer<Record>(RECORD_SEND_ADDR) { msg ->
                val record = msg.body()

                if (++receivedRecords % recordsPerBundle == 0) {
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
            }

            deploySingleRecordForwardConsumerTestVerticle(
                vertx,
                createKinesisConsumerVerticleConfig(errorHandling = ErrorHandling.IGNORE_AND_CONTINUE)
            )
            startConsumerVerticles(streamDescription)
            putRecords(recordBundles, recordsPerBundle)
        }
    }

    @Test
    internal fun consumer_ignore_on_failure_by_configuration(vertx: Vertx, testContext: VertxTestContext) {
        val recordBundles = 10
        val recordsPerBundle = 10
        val recordCount = recordBundles * recordsPerBundle

        asyncTest(testContext, recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            val recordSequenceNumbers = mutableListOf<String>()

            vertx.eventBus().consumer<Record>(RECORD_SEND_ADDR) { msg ->
                val record = msg.body()
                val sequenceNumber = record.sequenceNumber()
                testContext.verify { recordSequenceNumbers.shouldNotContain(sequenceNumber) }
                recordSequenceNumbers.add(sequenceNumber)

                // Each record processing will fail
                msg.fail(0, "")

                checkpoint.flag()
            }

            deploySingleRecordForwardConsumerTestVerticle(
                vertx,
                createKinesisConsumerVerticleConfig(
                    errorHandling = ErrorHandling.IGNORE_AND_CONTINUE,
                    recordsPerPoll = 1,
                    pollIntervalMillis = 10
                )
            )

            startConsumerVerticles(streamDescription)

            putRecords(recordBundles, recordsPerBundle)
        }
    }

    @Test
    internal fun consumer_ignore_on_failure_exception_override(vertx: Vertx, testContext: VertxTestContext) {
        val recordBundles = 10
        val recordsPerBundle = 10
        val recordCount = recordBundles * recordsPerBundle

        asyncTest(testContext, recordCount) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var recordSequenceNumber: String? = null

            vertx.eventBus().consumer<Record>(RECORD_SEND_ADDR) { msg ->
                // Every time the same / first record is excepted
                val record = msg.body()
                if (recordSequenceNumber.isNotNullOrBlank()) {
                    testContext.verify { recordSequenceNumber.shouldBe(record.sequenceNumber()) }
                } else {
                    recordSequenceNumber = record.sequenceNumber()
                }

                msg.replyConsumeRecordFailedIgnore(record)

                checkpoint.flag()
            }

            deploySingleRecordForwardConsumerTestVerticle(
                vertx,
                createKinesisConsumerVerticleConfig(
                    errorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
                    recordsPerPoll = 1,
                    pollIntervalMillis = 10
                )
            )

            startConsumerVerticles(streamDescription)

            putRecords(recordBundles, recordsPerBundle)
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
        val recordBundles = 10
        val recordsPerBundle = 10
        val retries = recordBundles * recordsPerBundle

        asyncTest(testContext, retries) { checkpoint ->
            val streamDescription = createAndGetStreamDescriptionWhenActive(1)

            var recordSequenceNumber: String? = null

            vertx.eventBus().consumer<Record>(RECORD_SEND_ADDR) { msg ->
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
            }

            deploySingleRecordForwardConsumerTestVerticle(
                vertx,
                createKinesisConsumerVerticleConfig(
                    errorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,
                    pollIntervalMillis = 10
                )
            )

            startConsumerVerticles(streamDescription)

            putRecords(recordBundles, recordsPerBundle)
        }
    }

    @Test
    internal fun shard_splitting(testContext: VertxTestContext) = asyncTest(testContext, 1) { checkpoint ->
        val parentShardId = ShardIdGenerator.generateShardId()

        val streamDescription = createAndGetStreamDescriptionWhenActive(1)

        deploySingleRecordForwardConsumerTestVerticle(vertx, createKinesisConsumerVerticleConfig())

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
                val existingParentShardIterator = shardStatePersistence.getShardIterator(parentShardId)

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
    internal fun shard_merging(testContext: VertxTestContext) = asyncTest(testContext, 1) { checkpoint ->
        val shardIds = ShardIdGenerator.generateShardIdList(3)
        val parentShardId = shardIds.first()
        val adjacentParentShardId = shardIds[1]
        // We need 2 shard to merge
        val streamDescription = createAndGetStreamDescriptionWhenActive(2)

        deploySingleRecordForwardConsumerTestVerticle(
            vertx,
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
                val parentShardIterator = shardStatePersistence.getShardIterator(parentShardId)
                val isAdjacentParentShardInProgress = shardStatePersistence.isShardInProgress(adjacentParentShardId)
                val adjacentParentShardIterator = shardStatePersistence.getShardIterator(adjacentParentShardId)

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
     * Start consumer verticle like orchestra verticle will do.
     */
    private suspend fun startConsumerVerticles(streamDescription: StreamDescription) {
        streamDescription.shards().forEach { shard ->
            vertx.eventBus()
                .requestAwait<Unit>(CONSUMER_START_CMD_ADDR, shard.shardId(), DeliveryOptions().setLocalOnly(true))
        }
    }

    private suspend fun deploySingleRecordForwardConsumerTestVerticle(
        vertx: Vertx,
        options: KinesisConsumerVerticleOptions = createKinesisConsumerVerticleConfig(),
        instances: Int = 1
    ) {
        deployVerticle(
            vertx,
            options,
            SingleRecordForwardConsumerTestVerticle::class.java.name,
            instances
        )
    }

    private suspend fun deployVerticle(
        vertx: Vertx,
        options: KinesisConsumerVerticleOptions,
        verticleClass: String,
        instances: Int = 1
    ) {
        deploymentId = vertx.deployVerticleAwait(
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
    override suspend fun onRecordsAsync(records: List<Record>) {
        records.forEach { record ->
            runCatching {
                vertx.eventBus().requestAwait<Unit>(KinesisConsumerVerticleTest.RECORD_SEND_ADDR, record)
            }.exceptionOrNull()?.let {
                throw if (it is ReplyException && it.isKinesisConsumerException()) {
                    it.toKinesisConsumerException()
                } else {
                    it
                }
            }
        }
    }
}
