package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_STREAM_NAME
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.createAndGetStreamDescriptionWhenActive
import io.kotest.assertions.throwables.shouldThrow
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException

internal class KinesisExtTest : AbstractKinesisAndRedisTest(false) {

    @Test
    internal fun streamDescriptionWhenActiveAwait_limit_exceeded(testContext: VertxTestContext) = testContext.async {
        kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val jobs = IntRange(1, 200).map {
            launch { kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) }
        }
        jobs.forEach { it.join() }
    }

    @Test
    internal fun listShardsRateLimitingAware_limit_exceeded(testContext: VertxTestContext) = testContext.async {
        kinesisClient.createAndGetStreamDescriptionWhenActive(1)
        val jobs = IntRange(1, 300).map {
            launch { kinesisClient.listShardsRateLimitingAware(TEST_STREAM_NAME) }
        }
        jobs.forEach { it.join() }
    }

    @Test
    internal fun streamDescriptionWhenActiveAwait_resource_not_found(testContext: VertxTestContext) = testContext.async {
        shouldThrow<ResourceNotFoundException> { kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME) }
    }

    @Test
    internal fun listShardsRateLimitingAware_resource_not_found(testContext: VertxTestContext) = testContext.async {
        shouldThrow<ResourceNotFoundException> { kinesisClient.listShardsRateLimitingAware(TEST_STREAM_NAME) }
    }
}