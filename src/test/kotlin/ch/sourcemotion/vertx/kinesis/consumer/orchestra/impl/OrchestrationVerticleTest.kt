package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestrationVerticleTest.Companion.RECORDS_RECEIVED_ACK_ADDR
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.bunchesOf
import io.kotest.matchers.ints.shouldBeBetween
import io.kotest.matchers.shouldBe
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
import kotlinx.coroutines.launch
import mu.KLogging
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record

internal class OrchestrationVerticleTest : AbstractKinesisAndRedisTest() {

    companion object : KLogging() {
        const val RECORDS_RECEIVED_ACK_ADDR = "/kinesis-consumer-orchester/testing/records-received/ack"
    }

    @Test
    internal fun streaming_one_shard(testContext: VertxTestContext) {
        val recordBunching = 10 bunchesOf 100

        val dataString = "record-data"
        val recordData = SdkBytes.fromUtf8String(dataString)

        asyncTest(testContext, recordBunching.recordCount) { checkpoint ->

            createAndGetStreamDescriptionWhenActive(1)

            eventBus.consumer<JsonArray>(RECORDS_RECEIVED_ACK_ADDR) {
                testContext.verify {
                    val recordsData = it.body()
                    recordsData.forEach { data ->
                        data.shouldBe(dataString)
                        checkpoint.flag()
                    }
                }
            }

            deployOrchestrationVerticle(vertx, LoadConfiguration.createExactConfig(1))

            putRecords(recordBunching, { recordData })
        }
    }

    @Test
    internal fun streaming_four_shards(testContext: VertxTestContext) {
        val recordBunching = 4 bunchesOf 250
        val dataString = "record-data"

        asyncTest(testContext, recordBunching.recordCount + 1) { checkpoint ->
            createAndGetStreamDescriptionWhenActive(recordBunching.recordBunches)

            val recordNumbers = ArrayList<Int>()
            eventBus.consumer<JsonArray>(RECORDS_RECEIVED_ACK_ADDR) {
                testContext.verify {
                    val recordsData = it.body()
                    recordsData.forEach { data ->
                        val recordNumber = data.toString().substringAfter("_").toInt()
                        testContext.verify {
                            recordNumber.shouldBeBetween(0, recordBunching.recordsPerBunch - 1)
                            recordNumbers.add(recordNumber)
                            checkpoint.flag()
                        }
                    }
                    if (recordNumbers.size == recordBunching.recordCount) {
                        testContext.verify {
                            repeat(recordBunching.recordsPerBunch) { recordNumber ->
                                recordNumbers.filter { it == recordNumber }.size.shouldBe(recordBunching.recordBunches)
                            }
                            checkpoint.flag()
                        }
                    }
                }
            }

            deployOrchestrationVerticle(vertx, LoadConfiguration.createDoAllShardsConfig())

            // We put the records with explicit hash key instead of partition key to ensure fair distribution between shards
            putRecordsExplicitHashKey(recordBunching, { SdkBytes.fromUtf8String("${dataString}_$it") })
        }
    }

    @Test
    internal fun streaming_and_resharding(testContext: VertxTestContext) {
        class Step(val shardIds: ShardIdList, val recordCountPerBundle: Int = 100, val reshardingAction: () -> Unit) {
            fun overAllRecordCount() = shardIds.size * recordCountPerBundle
        }

        val initialShardId = ShardIdGenerator.generateShardId()
        val splitChildShardIds = ShardIdGenerator.generateShardIdList(2, 1)
        val mergeChildShardId = ShardIdGenerator.generateShardId(3)


        val dataString = "record-data"

        // 100 on single shard before split, 200 after split, 100 after merge + 3 for finished steps
        asyncTest(testContext, 400 + 3) { checkpoint ->

            createAndGetStreamDescriptionWhenActive(1)

            val steps = listOf(
                Step(listOf(initialShardId)) {
                    defaultTestScope.launch {
                        kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME).let {
                            splitShardFair(it.shards().first())
                            logger.info { "Shard split done" }
                        }
                        checkpoint.flag()
                    }
                },
                Step(splitChildShardIds) {
                    defaultTestScope.launch {
                        kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME).let {
                            val mergeParent =
                                it.shards().first { shard -> shard.shardIdTyped() == splitChildShardIds.first() }
                            val mergeAdjacentParentShard =
                                it.shards().first { shard -> shard.shardIdTyped() == splitChildShardIds.last() }
                            mergeShards(mergeParent, mergeAdjacentParentShard)
                            logger.info { "Shard merge Merge done" }
                        }
                        checkpoint.flag()
                    }
                },
                Step(listOf(mergeChildShardId)) {
                    checkpoint.flag()
                }
            ).iterator()

            var step = steps.next()
            var receivedRecords = 0
            eventBus.consumer<JsonArray>(RECORDS_RECEIVED_ACK_ADDR) {
                val recordsData = it.body()
                repeat(recordsData.count()) { checkpoint.flag() }
                receivedRecords += recordsData.size()
                logger.info { "\"$receivedRecords\" record received" }
                if (receivedRecords == step.overAllRecordCount()) {
                    step.reshardingAction()
                    step = steps.next()
                }
            }

            val options = deployOrchestrationVerticle(vertx, LoadConfiguration.createDoAllShardsConfig())

            eventBus.consumer<Unit>(options.reshardingNotificationAddress) {
                logger.info { "Resharding notification received" }
                checkpoint.flag()
                defaultTestScope.launch {
                    putRecordsExplicitHashKey(
                        step.shardIds.size bunchesOf step.recordCountPerBundle,
                        { SdkBytes.fromUtf8String("${dataString}_$it") },
                        predefinedShards = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME).shards()
                            .filter { step.shardIds.contains(it.shardIdTyped()) }
                    )
                }
            }

            putRecordsExplicitHashKey(
                step.shardIds.size bunchesOf step.recordCountPerBundle,
                { SdkBytes.fromUtf8String("${dataString}_$it") },
                predefinedShards = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME).shards()
                    .filter { step.shardIds.contains(it.shardIdTyped()) }
            )
        }
    }

    private suspend fun deployOrchestrationVerticle(
        vertx: Vertx,
        loadConfiguration: LoadConfiguration
    ): OrchestrationVerticleOptions {
        val options =
            createOrchestrationVerticleOptions(
                RecordDataForwardKinesisConsumerTestVerticle::class.java.name,
                loadConfiguration
            )
        vertx.deployVerticleAwait(
            OrchestrationVerticle::class.java.name, DeploymentOptions().setConfig(JsonObject.mapFrom(options))
        )
        return options
    }

    private fun createOrchestrationVerticleOptions(
        verticleClassName: String,
        loadConfiguration: LoadConfiguration
    ) =
        VertxKinesisOrchestraOptions(
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            redisOptions = redisOptions,
            loadConfiguration = loadConfiguration,
            consumerVerticleClass = verticleClassName
        ).asOrchestraVerticleOptions()
}

class RecordDataForwardKinesisConsumerTestVerticle : AbstractKinesisConsumerCoroutineVerticle() {
    override suspend fun onRecordsAsync(records: List<Record>) {
        val recordsData = records.map { record -> record.data().asByteArray().toString(Charsets.UTF_8) }
        vertx.eventBus().send(RECORDS_RECEIVED_ACK_ADDR, JsonArray(recordsData))
    }
}
