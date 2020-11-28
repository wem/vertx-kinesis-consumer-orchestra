package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ComponentTest.FanoutMessage
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.LocalCodec
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import kotlinx.coroutines.delay
import mu.KLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Record
import kotlin.LazyThreadSafetyMode.NONE

internal class ComponentTest : AbstractKinesisAndRedisTest(false) {

    companion object : KLogging() {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val PARAMETER_VALUE = "consumer-parameter"
        const val RECORD_COUNT = 100
        const val NOT_CONSUMED_SHARD_DETECTION_INTERVAL = 500L
    }

    private var orchestra: VertxKinesisOrchestra? = null

    private val orchestraOptions by lazy(NONE) {
        VertxKinesisOrchestraOptions(
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            credentialsProviderSupplier = { Localstack.credentialsProvider },
            consumerVerticleClass = ComponentTestConsumerVerticle::class.java.name,
            redisOptions = redisHeimdallOptions,
            consumerVerticleOptions = JsonObject.mapFrom(ComponentTestConsumerOptions(PARAMETER_VALUE)),
            kinesisEndpoint = getKinesisEndpointOverride(),
            loadConfiguration = LoadConfiguration.createConsumeExact(1, NOT_CONSUMED_SHARD_DETECTION_INTERVAL)
        )
    }

    @BeforeEach
    internal fun setUpComponent(testContext: VertxTestContext) = asyncTest(testContext) {
        vertx.eventBus().registerDefaultCodec(FanoutMessage::class.java, LocalCodec("fanout-message-code"))
        kinesisClient.createAndGetStreamDescriptionWhenActive(1)

        orchestra = VertxKinesisOrchestra.create(vertx, orchestraOptions).startAwait()
    }

    @AfterEach
    internal fun closeOrchestra(testContext: VertxTestContext) = asyncTest(testContext) {
        orchestra?.closeAwait()
    }

    @Test
    internal fun consume_some_records(testContext: VertxTestContext) =
        testContext.async(RECORD_COUNT) { checkpoint ->
            eventBus.consumer<FanoutMessage>(RECORD_FAN_OUT_ADDR) { msg ->
                val fanoutMessage = msg.body()
                logger.info { "${fanoutMessage.recordCount} records received" }
                repeat(fanoutMessage.recordCount) { checkpoint.flag() }
                testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
            }.completionHandlerAwait()

            // We wait until consumer are deployed, so latest shard iterator will work
            delay(orchestraOptions.loadConfiguration.notConsumedShardDetectionInterval * 2)

            kinesisClient.putRecords(1 batchesOf RECORD_COUNT)
        }

    data class FanoutMessage(val recordCount: Int, val parameter: String)
}


class ComponentTestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {

    private val options: ComponentTestConsumerOptions by lazy {
        config.mapTo(ComponentTestConsumerOptions::class.java)
    }

    override suspend fun onRecordsAsync(records: List<Record>) {
        val msg = FanoutMessage(records.size, options.someParameter)
        vertx.eventBus().send(ComponentTest.RECORD_FAN_OUT_ADDR, msg)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class ComponentTestConsumerOptions(val someParameter: String)
