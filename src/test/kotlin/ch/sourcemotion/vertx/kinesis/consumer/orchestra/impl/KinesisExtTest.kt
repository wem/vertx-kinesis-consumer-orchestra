package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_STREAM_NAME
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.createAndGetStreamDescriptionWhenActive
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test

internal class KinesisExtTest : AbstractKinesisAndRedisTest(false) {

    @Test
    internal fun streamDescriptionWhenActiveAwait_limit_exceeded(testContext: VertxTestContext) = testContext.async {
        kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val jobs = IntRange(1, 200).map {
            defaultTestScope.launch { kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) }
        }
        jobs.forEach { it.join() }
    }

    @Test
    internal fun listShardsSafe_limit_exceeded(testContext: VertxTestContext) = testContext.async {
        kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val jobs = IntRange(1, 300).map {
            defaultTestScope.launch { kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) }
        }
        jobs.forEach { it.join() }
    }
}