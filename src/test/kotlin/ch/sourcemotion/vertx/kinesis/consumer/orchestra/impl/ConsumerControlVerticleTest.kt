package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.RecordDataForwardKinesisConsumerTestVerticle.Companion.RECORDS_RECEIVED_ACK_ADDR
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StartConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.cmd.StopConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.ints.shouldBeBetween
import io.kotest.matchers.shouldBe
import io.vertx.core.Handler
import io.vertx.core.Verticle
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.eventbus.requestAwait
import kotlinx.coroutines.launch
import mu.KLogging
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record

internal class ConsumerControlVerticleTest : AbstractKinesisAndRedisTest() {

    companion object : KLogging() {
        const val DATA_STRING = "record-data"
    }

    @Test
    internal fun start_one_consumer(testContext: VertxTestContext) {
        val recordBunching = 10 batchesOf 100
        val recordData = SdkBytes.fromUtf8String(DATA_STRING)

        testContext.async(recordBunching.recordCount + 2) { checkpoint ->

            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(1)

            eventBus.consumer<JsonArray>(RECORDS_RECEIVED_ACK_ADDR) {
                testContext.verify {
                    val recordsData = it.body()
                    recordsData.forEach { data ->
                        data.shouldBe(DATA_STRING)
                        checkpoint.flag()
                    }
                }
            }

            val activeConsumerCountVerifier =
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint)
                    .addVerifier(2, 1)

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            deployConsumerControl(LoadConfiguration.createConsumeAllShards())
            sendStartConsumerCmds(streamDescription.shardIds())

            kinesisClient.putRecords(recordBunching, recordDataSupplier = { recordData })
        }
    }

    @Test
    internal fun start_four_consumers(testContext: VertxTestContext) {
        val recordBatches = 4 batchesOf 250

        asyncTest(testContext, recordBatches.recordCount + 3) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(recordBatches.recordBatches)

            val consumedRecordNumbers = ArrayList<Int>()
            eventBus.consumer<JsonArray>(RECORDS_RECEIVED_ACK_ADDR) {
                testContext.verify {
                    val recordsData = it.body()
                    recordsData.forEach { data ->
                        val recordNumber = data.toString().substringAfter("_").toInt()
                        testContext.verify {
                            recordNumber.shouldBeBetween(0, recordBatches.recordsPerBatch - 1)
                            consumedRecordNumbers.add(recordNumber)
                            checkpoint.flag()
                        }
                    }
                    // Verify all records on each shard got consumed
                    if (consumedRecordNumbers.size == recordBatches.recordCount) {
                        testContext.verify {
                            repeat(recordBatches.recordsPerBatch) { recordNumber ->
                                consumedRecordNumbers.filter { it == recordNumber }.size.shouldBe(recordBatches.recordBatches)
                            }
                            checkpoint.flag()
                        }
                    }
                }
            }

            val activeConsumerCountVerifier = ConsumedShardCountVerifier
                .withStartNotificationVerifier(testContext, checkpoint)
                .addSequentialVerifier(2)
                .addSequentialVerifier(3)
                .addSequentialVerifier(4)
                .addSequentialVerifier(5)

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            deployConsumerControl(LoadConfiguration.createConsumeAllShards())
            sendStartConsumerCmds(streamDescription.shardIds())

            // We put the records with explicit hash key instead of partition key to ensure fair distribution between shards
            kinesisClient.putRecordsExplicitHashKey(recordBatches, { SdkBytes.fromUtf8String("${DATA_STRING}_$it") })
        }
    }

    @Test
    internal fun start_consumers(testContext: VertxTestContext) = testContext.async(2) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)

        val activeConsumerCountVerifier =
            ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint)
                .addSequentialVerifier(2)
                .addSequentialVerifier(3)

        eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

        deployConsumerControl(LoadConfiguration.createConsumeAllShards())
        streamDescription.shardIds().forEach { sendStartConsumerCmds(listOf(it)) }
    }

    @Test
    internal fun stop_consumers(testContext: VertxTestContext) = testContext.asyncDelayed(5) { checkpoint ->
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)

        val activeConsumerCountVerifier = ConsumedShardCountVerifier
            .withStartNotificationVerifier(testContext, checkpoint)
            .addSequentialVerifier(2)
            .addVerifier(3) { msg ->
                verify { msg.body().shouldBe(2) }
                defaultTestScope.launch { streamDescription.shardIds().forEach { stopConsumer(it) } }
                checkpoint.flag()
            }
            .addVerifier(4, 1)
            .addVerifier(5, 0)

        eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

        deployConsumerControl(LoadConfiguration.createConsumeAllShards())
        sendStartConsumerCmds(streamDescription.shardIds())
    }

    @Test
    internal fun start_empty_consumer_list_will_reply_direct(testContext: VertxTestContext) =
        testContext.asyncDelayed(1) { checkpoint ->
            kinesisClient.createAndGetStreamDescriptionWhenActive()

            eventBus.consumer(
                EventBusAddr.detection.consumedShardCountNotification,
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint)
            )

            deployConsumerControl(LoadConfiguration.createConsumeAllShards())
            sendStartConsumerCmds(emptyList())
        }

    @Test
    internal fun try_start_more_consumer_than_capacity(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(4)

            val activeConsumerCountVerifier =
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint)
                    .addSequentialVerifier(2)
                    .addVerifier(3, 2)

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            deployConsumerControl(LoadConfiguration.createConsumeExact(2))
            sendStartConsumerCmds(streamDescription.shardIds())
        }

    @Test
    internal fun try_start_consumers_already_consumed_shard(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)

            val activeConsumerCountVerifier =
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint).addVerifier(2, 1)

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            shardStatePersistenceService.flagShardInProgress(streamDescription.shardIds().first())

            deployConsumerControl(LoadConfiguration.createConsumeExact(2))
            sendStartConsumerCmds(streamDescription.shardIds())
        }

    @Test
    internal fun try_start_consumers_already_finished_shard(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)

            val activeConsumerCountVerifier =
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint).addVerifier(2, 1)

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            shardStatePersistenceService.saveFinishedShard(streamDescription.shardIds().first(), 10000)

            deployConsumerControl(LoadConfiguration.createConsumeExact(2))
            sendStartConsumerCmds(streamDescription.shardIds())
        }

    @Test
    internal fun try_start_consumers_already_finished_and_consumed_shards(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(3)

            val activeConsumerCountVerifier =
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint).addVerifier(2, 1)

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            shardStatePersistenceService.flagShardInProgress(streamDescription.shardIds()[0])
            shardStatePersistenceService.saveFinishedShard(streamDescription.shardIds()[1], 10000)

            deployConsumerControl(LoadConfiguration.createConsumeExact(3))
            sendStartConsumerCmds(streamDescription.shardIds())
        }

    @Test
    internal fun try_start_consumers_with_zero_capacity(testContext: VertxTestContext) =
        testContext.async(1) { checkpoint ->
            val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(2)

            val activeConsumerCountVerifier =
                ConsumedShardCountVerifier.withStartNotificationVerifier(testContext, checkpoint)
                    .addVerifier(2) { msg ->
                        verify {
                            msg.body().shouldBe(1)
                        }
                        // Start a consumer that exceed capacity, and no left shards
                        defaultTestScope.launch {
                            shouldThrow<ReplyException> {
                                sendStartConsumerCmds(
                                    streamDescription.shardIds().takeLast(1)
                                )
                            }
                            checkpoint.flag()
                        }
                    }

            eventBus.consumer(EventBusAddr.detection.consumedShardCountNotification, activeConsumerCountVerifier)

            deployConsumerControl(LoadConfiguration.createConsumeExact(1))
            sendStartConsumerCmds(streamDescription.shardIds().take(1))
        }

    private suspend fun sendStartConsumerCmds(shardIds: ShardIdList) {
        shardIds.forEach { shardId ->
            val cmd = StartConsumerCmd(shardId, ShardIteratorStrategy.EXISTING_OR_LATEST)
            eventBus.requestAwait<Unit>(EventBusAddr.consumerControl.startConsumerCmd, cmd)
        }
    }

    private suspend fun stopConsumer(shardId: ShardId) {
        val cmd = StopConsumerCmd(shardId)
        eventBus.requestAwait<Unit>(EventBusAddr.consumerControl.stopConsumerCmd, cmd)
    }

    private suspend fun deployConsumerControl(
        loadConfiguration: LoadConfiguration,
        verticleOptions: JsonObject = JsonObject()
    ): ConsumerControlVerticle.Options {
        val options =
            consumerControlOptionsOf(
                RecordDataForwardKinesisConsumerTestVerticle::class.java,
                loadConfiguration,
                verticleOptions
            )
        return deployConsumerControl(options)
    }

    private suspend fun deployConsumerControl(
        options: ConsumerControlVerticle.Options
    ): ConsumerControlVerticle.Options {
        deployTestVerticle<ConsumerControlVerticle>(options)
        return options
    }

    private fun consumerControlOptionsOf(
        verticleClass: Class<out Verticle>,
        loadConfiguration: LoadConfiguration,
        verticleOptions: JsonObject = JsonObject()
    ) =
        VertxKinesisOrchestraOptions(
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            redisOptions = redisHeimdallOptions,
            loadConfiguration = loadConfiguration,
            consumerVerticleClass = verticleClass.name,
            consumerVerticleOptions = verticleOptions
        ).asConsumerControlOptions()
}

class RecordDataForwardKinesisConsumerTestVerticle : AbstractKinesisConsumerCoroutineVerticle() {

    companion object {
        const val RECORDS_RECEIVED_ACK_ADDR = "/kinesis-consumer-orchester/testing/records-received/ack"
    }

    override suspend fun onRecordsAsync(records: List<Record>) {
        val recordsData = records.map { record -> record.data().asByteArray().toString(Charsets.UTF_8) }
        vertx.eventBus().send(RECORDS_RECEIVED_ACK_ADDR, JsonArray(recordsData))
    }
}

private class ConsumedShardCountVerifier private constructor(
    private val testContext: VertxTestContext,
    private val checkpoint: Checkpoint
) : Handler<Message<Int>> {

    companion object {
        fun withStartNotificationVerifier(testContext: VertxTestContext, checkpoint: Checkpoint) =
            ConsumedShardCountVerifier(testContext, checkpoint).apply {
                addDetectionStartNotificationVerifier()
            }
    }

    private var eventNumber = 0
    private val verifiers = HashMap<Int, VertxTestContext.(Message<Int>) -> Unit>()

    private fun addDetectionStartNotificationVerifier() {
        addVerifier(1, 0)
    }

    fun addSequentialVerifier(eventNumber: Int): ConsumedShardCountVerifier = addVerifier(eventNumber, eventNumber - 1)

    fun addVerifier(eventNumber: Int, expectedActiveConsumers: Int): ConsumedShardCountVerifier {
        verifiers[eventNumber] = { msg ->
            verify { msg.body().shouldBe(expectedActiveConsumers) }
            checkpoint.flag()
        }
        return this
    }

    fun addVerifier(eventNumber: Int, verifier: VertxTestContext.(Message<Int>) -> Unit): ConsumedShardCountVerifier {
        verifiers[eventNumber] = verifier
        return this
    }

    override fun handle(msg: Message<Int>) {
        val eventNumber = ++eventNumber
        val verifier = verifiers[eventNumber]
        if (verifier != null) {
            verifier(testContext, msg)
        } else {
            testContext.failNow(Exception("No active consumer verifier on notification event number \"$eventNumber\""))
        }
    }
}
