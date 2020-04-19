package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.MergeReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.SplitReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Record
import java.util.function.Supplier

internal class ComponentTest : AbstractKinesisAndRedisTest() {

    companion object {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val PARAMETER_VALUE = "consumer-paramater"
        const val RECORD_COUNT = 100
    }

    private lateinit var sut: VertxKinesisOrchestra

    @BeforeEach
    internal fun setUpComponent(testContext: VertxTestContext) = asyncTest(testContext) {
        eventBus.unregisterDefaultCodec(MergeReshardingEvent::class.java)
        eventBus.unregisterDefaultCodec(SplitReshardingEvent::class.java)

        createAndGetStreamDescriptionWhenActive(1)
    }

    @Test
    internal fun consumer_some_records(testContext: VertxTestContext) =
        asyncTest(testContext, RECORD_COUNT) { checkpoint ->
            sut = VertxKinesisOrchestra.create(
                vertx,
                VertxKinesisOrchestraOptions(
                    TEST_APPLICATION_NAME,
                    TEST_STREAM_NAME,
                    credentialsProviderSupplier = Supplier { CREDENTIALS_PROVIDER },
                    consumerVerticleClass = ComponentTestConsumerVerticle::class.java.name,
                    redisOptions = redisOptions,
                    consumerVerticleConfig = JsonObject.mapFrom(ComponentTestConsumerOptions(PARAMETER_VALUE)),
                    kinesisEndpoint = getKinesisEndpoint()
                )
            ).startAwait()

            eventBus.consumer<JsonObject>(RECORD_FAN_OUT_ADDR) { msg ->
                val fanoutMessage = msg.body().mapTo(FanoutMessage::class.java)
                repeat(fanoutMessage.recordCount) { checkpoint.flag() }
                testContext.verify { fanoutMessage.parameter.shouldBe(PARAMETER_VALUE) }
            }

            putRecords(1, RECORD_COUNT)
        }
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
data class ComponentTestConsumerOptions(
    val someParameter: String
)

data class FanoutMessage(val recordCount: Int, val parameter: String)
