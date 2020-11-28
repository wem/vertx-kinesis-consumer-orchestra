package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ComponentTest.FanoutMessage
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Record

internal class ComponentTest : AbstractKinesisAndRedisTest(false) {

    companion object {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val PARAMETER_VALUE = "consumer-parameter"
        const val RECORD_COUNT = 100
    }

    private var orchestra: VertxKinesisOrchestra? = null

    @BeforeEach
    internal fun setUpComponent(testContext: VertxTestContext) = asyncTest(testContext) {
        kinesisClient.createAndGetStreamDescriptionWhenActive(1)

        orchestra = VertxKinesisOrchestra.create(
            vertx, VertxKinesisOrchestraOptions(
                TEST_APPLICATION_NAME,
                TEST_STREAM_NAME,
                credentialsProviderSupplier = { Localstack.credentialsProvider },
                consumerVerticleClass = ComponentTestConsumerVerticle::class.java.name,
                redisOptions = redisHeimdallOptions,
                consumerVerticleConfig = JsonObject.mapFrom(ComponentTestConsumerOptions(PARAMETER_VALUE)),
                kinesisEndpoint = localStackContainer.getKinesisEndpointOverride()
            )
        ).startAwait()
    }

    @AfterEach
    internal fun closeOrchestra(testContext: VertxTestContext) = asyncTest(testContext) {
        orchestra?.closeAwait()
    }

    @Test
    internal fun consume_some_records(testContext: VertxTestContext) =
        asyncTest(testContext, RECORD_COUNT) { checkpoint ->
            eventBus.consumer<JsonObject>(RECORD_FAN_OUT_ADDR) { msg ->
                val fanoutMessage = msg.body().mapTo(FanoutMessage::class.java)
                logger.info { "${fanoutMessage.recordCount} records received" }
                repeat(fanoutMessage.recordCount) { checkpoint.flag() }
                testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
            }

            kinesisClient.putRecords(1 bunchesOf RECORD_COUNT)
        }

    data class FanoutMessage(val recordCount: Int, val parameter: String)
}


class ComponentTestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {

    private val options: ComponentTestConsumerOptions by lazy {
        config.mapTo(ComponentTestConsumerOptions::class.java)
    }

    override suspend fun onRecordsAsync(records: List<Record>) {
        val msg = FanoutMessage(records.size, options.someParameter)
        vertx.eventBus().send(ComponentTest.RECORD_FAN_OUT_ADDR, JsonObject.mapFrom(msg))
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class ComponentTestConsumerOptions(val someParameter: String)
