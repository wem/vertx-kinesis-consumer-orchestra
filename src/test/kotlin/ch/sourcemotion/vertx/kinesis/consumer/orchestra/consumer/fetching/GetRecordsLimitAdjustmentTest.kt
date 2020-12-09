package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.DynamicLimitAdjustment
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
import software.amazon.awssdk.services.kinesis.model.Record

internal class GetRecordsLimitAdjustmentTest {

    @Test
    internal fun same_limit_until_started() {
        val options =
            FetcherOptions(dynamicLimitAdjustment = DynamicLimitAdjustment(getRecordResultsToStartAdjustment = 2))
        val expectedLimitAdjustmentNotStarted = options.getRecordsLimit
        val expectedLimitAdjustmentStarted =
            expectedLimitAdjustmentNotStarted - options.dynamicLimitAdjustment.limitDecreaseStep
        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.calculateNextLimit().shouldBe(expectedLimitAdjustmentNotStarted)
        sut.includeResponse(getRecordsResponseWithRecords(1))
        sut.calculateNextLimit().shouldBe(expectedLimitAdjustmentNotStarted)
        sut.includeResponse(getRecordsResponseWithRecords(1))
        sut.calculateNextLimit().shouldBe(expectedLimitAdjustmentStarted)
    }

    @Test
    internal fun increase_threshold_not_reached() {
        val increaseThreshold = 100
        val initialGetRecordsLimit = 500
        val options = FetcherOptions(
            dynamicLimitAdjustment = DynamicLimitAdjustment(
                getRecordResultsToStartAdjustment = 1,
                limitIncreaseThreshold = increaseThreshold,
            ), getRecordsLimit = initialGetRecordsLimit
        )

        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.includeResponse(getRecordsResponseWithRecords(400)) // threshold is greater than, not equals or greather
        sut.calculateNextLimit().shouldBe(initialGetRecordsLimit)
    }

    @Test
    internal fun increase_threshold_reached() {
        val increaseThreshold = 100
        val increaseStep = 50
        val initialGetRecordsLimit = 500
        val options = FetcherOptions(
            dynamicLimitAdjustment = DynamicLimitAdjustment(
                getRecordResultsToStartAdjustment = 1,
                limitIncreaseThreshold = increaseThreshold,
                limitIncreaseStep = increaseStep
            ), getRecordsLimit = initialGetRecordsLimit
        )

        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.includeResponse(getRecordsResponseWithRecords(401))
        sut.calculateNextLimit().shouldBe(initialGetRecordsLimit + increaseStep)
    }

    @Test
    internal fun increase_not_exceeds_maximum() {
        val increaseThreshold = 100
        val increaseStep = 201 // 1 more than max
        val initialGetRecordsLimit = 9800
        val options = FetcherOptions(
            dynamicLimitAdjustment = DynamicLimitAdjustment(
                getRecordResultsToStartAdjustment = 1,
                limitIncreaseThreshold = increaseThreshold,
                limitIncreaseStep = increaseStep
            ), getRecordsLimit = initialGetRecordsLimit
        )

        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.includeResponse(getRecordsResponseWithRecords(9901))
        sut.calculateNextLimit().shouldBe(GetRecordsLimitAdjustment.MAX_ALLOWED_LIMIT)
    }

    @Test
    internal fun decrease_threshold_not_reached() {
        val decreaseThreshold = 300
        val initialGetRecordsLimit = 500
        val options = FetcherOptions(
            dynamicLimitAdjustment = DynamicLimitAdjustment(
                getRecordResultsToStartAdjustment = 1,
                limitDecreaseThreshold = decreaseThreshold,
            ), getRecordsLimit = initialGetRecordsLimit
        )

        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.includeResponse(getRecordsResponseWithRecords(200)) // threshold is greater than, not equals or greather
        sut.calculateNextLimit().shouldBe(initialGetRecordsLimit)
    }

    @Test
    internal fun decrease_threshold_reached() {
        val decreaseThreshold = 300
        val decreaseStep = 50
        val initialGetRecordsLimit = 500
        val options = FetcherOptions(
            dynamicLimitAdjustment = DynamicLimitAdjustment(
                getRecordResultsToStartAdjustment = 1,
                limitDecreaseThreshold = decreaseThreshold,
                limitDecreaseStep = decreaseStep
            ), getRecordsLimit = initialGetRecordsLimit
        )

        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.includeResponse(getRecordsResponseWithRecords(199))
        sut.calculateNextLimit().shouldBe(initialGetRecordsLimit - decreaseStep)
    }

    @Test
    internal fun decrease_not_exceeds_maximum() {
        val minimalLimit = 200
        val decreaseThreshold = 300
        val decreaseStep = 301 // 1 more than min
        val initialGetRecordsLimit = 500
        val options = FetcherOptions(
            dynamicLimitAdjustment = DynamicLimitAdjustment(
                getRecordResultsToStartAdjustment = 1,
                limitDecreaseThreshold = decreaseThreshold,
                limitDecreaseStep = decreaseStep,
                minimalLimit = minimalLimit
            ), getRecordsLimit = initialGetRecordsLimit
        )

        val sut = GetRecordsLimitAdjustment.withOptions("some-stream", ShardId("test-shard"), options)
        sut.includeResponse(getRecordsResponseWithRecords(199))
        sut.calculateNextLimit().shouldBe(minimalLimit)
    }

    private fun getRecordsResponseWithRecords(recordCount: Int) = mock<GetRecordsResponse> {
        val records = IntRange(1, recordCount).map { mock<Record>() }
        on { records() } doReturn records
    }
}
