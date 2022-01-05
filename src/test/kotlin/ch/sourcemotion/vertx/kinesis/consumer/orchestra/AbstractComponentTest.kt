package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.LocalCodec
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import mu.KLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.util.concurrent.TimeUnit

internal abstract class AbstractComponentTest : AbstractKinesisAndRedisTest(false) {

    companion object : KLogging() {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val PARAMETER_VALUE = "consumer-parameter"
        const val RECORD_COUNT = 100
    }

    private var orchestra: VertxKinesisOrchestra? = null


    @BeforeEach
    internal fun setUpComponent() = asyncBeforeOrAfter {
        vertx.eventBus().registerDefaultCodec(FanoutMessage::class.java, LocalCodec("fanout-message-codec"))
    }

    @AfterEach
    fun closeOrchestra() = asyncBeforeOrAfter {
        orchestra?.close()?.await()
    }

    @Test
    internal fun consume_some_records(testContext: VertxTestContext) = testContext.async(RECORD_COUNT) { checkpoint ->
        createStreamAndDeployVKCO()
        eventBus.consumer<FanoutMessage>(RECORD_FAN_OUT_ADDR) { msg ->
            val fanoutMessage = msg.body()
            repeat(fanoutMessage.recordCount) { checkpoint.flag() }
            testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
        }.completion().await()

        val nodeScoreService = NodeScoreService.createService(vertx)
        nodeScoreService.awaitScore(1)

        kinesisClient.putRecords(1 batchesOf RECORD_COUNT)
    }

    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_resharding(testContext: VertxTestContext) {
        val initialShardCount = 4
        val splitShardCount = initialShardCount * 2

        testContext.async(RECORD_COUNT * (initialShardCount + splitShardCount)) { checkpoint ->
            eventBus.consumer<FanoutMessage>(RECORD_FAN_OUT_ADDR) { msg ->
                val fanoutMessage = msg.body()
                testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
                repeat(fanoutMessage.recordCount) { checkpoint.flag() }
            }.completion().await()

            val nodeScoreService = NodeScoreService.createService(vertx)
            val streamDescriptionBeforeSplit = createStreamAndDeployVKCO(initialShardCount)

            nodeScoreService.awaitScore(initialShardCount)

            val parentShards = streamDescriptionBeforeSplit.shards()
            kinesisClient.putRecordsExplicitHashKey(4 batchesOf RECORD_COUNT, predefinedShards = parentShards)

            val childShards = parentShards.map { parentShard ->
                kinesisClient.splitShardFair(parentShard)
                val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
                streamDescriptionAfterSplit.shards().filter { it.parentShardId() == parentShard.shardId() }.also {
                    logger.info { "Split of parentShard ${parentShard.shardId()} done" }
                }
            }.flatten()

            nodeScoreService.awaitScore(splitShardCount)

            kinesisClient.putRecordsExplicitHashKey(childShards.size batchesOf RECORD_COUNT, predefinedShards = childShards)
        }
    }

    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun merge_resharding(testContext: VertxTestContext) {
        val initialShardCount = 4
        val mergedShardCount = initialShardCount / 2
        testContext.async(RECORD_COUNT * (initialShardCount + mergedShardCount)) { checkpoint ->
            eventBus.consumer<FanoutMessage>(RECORD_FAN_OUT_ADDR) { msg ->
                val fanoutMessage = msg.body()
                testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
                repeat(fanoutMessage.recordCount) { checkpoint.flag() }
            }.completion().await()

            val nodeScoreService = NodeScoreService.createService(vertx)
            val streamDescriptionBeforeMerge = createStreamAndDeployVKCO(initialShardCount)

            val parentShards = streamDescriptionBeforeMerge.shards()
            nodeScoreService.awaitScore(parentShards.size)

            kinesisClient.putRecordsExplicitHashKey(initialShardCount batchesOf RECORD_COUNT, predefinedShards = parentShards)

            val shards = parentShards.iterator()
            val childShards = ArrayList<Shard>()
            while (shards.hasNext()) {
                val parent = shards.next()
                val adjacentParent = shards.next()
                kinesisClient.mergeShards(parent, adjacentParent)
                val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
                val childShard = streamDescriptionAfterSplit.shards()
                    .first { it.parentShardId() == parent.shardId() && it.adjacentParentShardId() == adjacentParent.shardId() }
                childShards.add(childShard)

                logger.info { "Merge of shards ${parent.shardId()} / ${adjacentParent.shardId()} done" }
            }

            nodeScoreService.awaitScore(mergedShardCount)
            delay(30000) // Because of some reason, the hash key range could change for some time after merge
            kinesisClient.putRecordsExplicitHashKey( mergedShardCount batchesOf RECORD_COUNT, predefinedShards = childShards)
        }
    }

    private suspend fun createStreamAndDeployVKCO(shardCount: Int = 1): StreamDescription {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount)
        val orchestraOptions = VertxKinesisOrchestraOptions(
            applicationName = TEST_APPLICATION_NAME,
            streamName = TEST_STREAM_NAME,
            region = AWS_REGION,
            credentialsProviderSupplier = { AWS_CREDENTIALS_PROVIDER },
            consumerVerticleClass = ComponentTestConsumerVerticle::class.java.name,
            redisOptions = redisHeimdallOptions,
            consumerVerticleOptions = JsonObject.mapFrom(ComponentTestConsumerOptions(PARAMETER_VALUE)),
            fetcherOptions = fetcherOptions(streamDescription)
        )

        orchestra = VertxKinesisOrchestra.create(vertx, orchestraOptions).start().await()
        return streamDescription
    }

    protected abstract fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions

    private suspend fun NodeScoreService.awaitScore(expectedScore : Int) {
        var expectedScoreReached = false
        while (!expectedScoreReached) {
            expectedScoreReached = getNodeScores().await().sumOf { it.score } == expectedScore
            delay(1000)
        }
    }
}

data class FanoutMessage(val recordCount: Int, val parameter: String)

class ComponentTestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {

    private val options: ComponentTestConsumerOptions by lazy {
        config.mapTo(ComponentTestConsumerOptions::class.java)
    }

    override suspend fun onRecordsAsync(records: List<Record>) {
        val msg = FanoutMessage(records.size, options.someParameter)
        vertx.eventBus().send(AbstractComponentTest.RECORD_FAN_OUT_ADDR, msg)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class ComponentTestConsumerOptions(val someParameter: String)
