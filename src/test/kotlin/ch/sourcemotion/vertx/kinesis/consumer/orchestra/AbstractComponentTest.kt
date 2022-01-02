package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.LocalCodec
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
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
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.util.concurrent.TimeUnit

internal abstract class AbstractComponentTest : AbstractKinesisAndRedisTest(false) {

    companion object : KLogging() {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val PARAMETER_VALUE = "consumer-parameter"
        const val RECORD_COUNT = 100
        const val NOT_CONSUMED_SHARD_DETECTION_INTERVAL = 500L
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

        delay(10000)
        kinesisClient.putRecords(1 batchesOf RECORD_COUNT)
    }

    @Timeout(value = 240, timeUnit = TimeUnit.SECONDS)
    @Test
    internal fun split_resharding(testContext: VertxTestContext) = testContext.async(RECORD_COUNT * 12) { checkpoint ->
        var receivedRecords = 0
        eventBus.consumer<FanoutMessage>(RECORD_FAN_OUT_ADDR) { msg ->
            val fanoutMessage = msg.body()
            receivedRecords += fanoutMessage.recordCount
            repeat(fanoutMessage.recordCount) { checkpoint.flag() }
            testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
        }.completion().await()

        val streamDescriptionBeforeSplit = createStreamAndDeployVKCO(4, 8)

        delay(40000)

        val parentShards = streamDescriptionBeforeSplit.shards()
        kinesisClient.putRecordsExplicitHashKey(4 batchesOf RECORD_COUNT, predefinedShards = parentShards)
        println("CHECK")

        parentShards.forEach { parentShard ->
            kinesisClient.splitShardFair(parentShard)
            delay(40000) // We have to wait until parent parentShard get really closed
            val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
            val childShards = streamDescriptionAfterSplit.shards().filter { it.parentShardId() == parentShard.shardId() }
            kinesisClient.putRecordsExplicitHashKey(childShards.size batchesOf RECORD_COUNT, predefinedShards = childShards)
            logger.info { "Split of parentShard ${parentShard.shardId()} done" }
        }
    }

    @Timeout(value = 120, timeUnit = TimeUnit.SECONDS)
    @Test
    internal fun merge_resharding(testContext: VertxTestContext) = testContext.async(RECORD_COUNT * 6) { checkpoint ->
        var receivedRecords = 0
        eventBus.consumer<FanoutMessage>(RECORD_FAN_OUT_ADDR) { msg ->
            val fanoutMessage = msg.body()
            receivedRecords += fanoutMessage.recordCount
            repeat(fanoutMessage.recordCount) { checkpoint.flag() }
            testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
        }.completion().await()

        val streamDescriptionBeforeMerge = createStreamAndDeployVKCO(4, 4)

        delay(20000)

        val parentShards = streamDescriptionBeforeMerge.shards()
        kinesisClient.putRecordsExplicitHashKey(4 batchesOf RECORD_COUNT, predefinedShards = parentShards)

        val shards = parentShards.iterator()
        while (shards.hasNext()) {
            val parent = shards.next()
            val adjacentParent = shards.next()
            kinesisClient.mergeShards(parent, adjacentParent)
            delay(20000) // We have to wait until parent shard get really closed
            val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
            val childShard = streamDescriptionAfterSplit.shards()
                .first { it.parentShardId() == parent.shardId() && it.adjacentParentShardId() == adjacentParent.shardId() }
            kinesisClient.putRecordsExplicitHashKey(1 batchesOf RECORD_COUNT, predefinedShards = listOf(childShard))

            logger.info { "Merge of shards ${parent.shardId()} / ${adjacentParent.shardId()} done" }
        }
    }

    private suspend fun createStreamAndDeployVKCO(shardCount: Int = 1, consumerCount: Int = 1): StreamDescription {
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
