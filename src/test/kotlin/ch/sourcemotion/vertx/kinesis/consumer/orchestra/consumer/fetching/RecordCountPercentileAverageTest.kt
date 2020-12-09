package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.DynamicLimitAdjustmentPercentileOrAverage
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RecordCountPercentileAverageTest {

    @Test
    internal fun p50_calculation() {
        val sut = RecordCountPercentileAverage(DynamicLimitAdjustmentPercentileOrAverage.P50)
        sut.calculatePercentileAverage(listOf(50)).shouldBe(50)
        sut.calculatePercentileAverage(listOf(100, 50, 200)).shouldBe(100)
        sut.calculatePercentileAverage(listOf(100, 50, 200).sortedDescending()).shouldBe(100)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300)).shouldBe(175)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300).sortedDescending()).shouldBe(175)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }).shouldBe(225)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }.sortedDescending()).shouldBe(225)
    }

    @Test
    internal fun p80_calculation() {
        val sut = RecordCountPercentileAverage(DynamicLimitAdjustmentPercentileOrAverage.P80)
        sut.calculatePercentileAverage(listOf(50)).shouldBe(50)
        sut.calculatePercentileAverage(listOf(100, 50, 200)).shouldBe(200)
        sut.calculatePercentileAverage(listOf(100, 50, 200).sortedDescending()).shouldBe(200)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300)).shouldBe(250)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300).sortedDescending()).shouldBe(250)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }).shouldBe(375)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }.sortedDescending()).shouldBe(375)
    }

    @Test
    internal fun p90_calculation() {
        val sut = RecordCountPercentileAverage(DynamicLimitAdjustmentPercentileOrAverage.P90)
        sut.calculatePercentileAverage(listOf(50)).shouldBe(50)
        sut.calculatePercentileAverage(listOf(100, 50, 200)).shouldBe(200)
        sut.calculatePercentileAverage(listOf(100, 50, 200).sortedDescending()).shouldBe(200)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300)).shouldBe(300)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300).sortedDescending()).shouldBe(300)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }).shouldBe(425)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }.sortedDescending()).shouldBe(425)
    }

    @Test
    internal fun average_calculation() {
        val sut = RecordCountPercentileAverage(DynamicLimitAdjustmentPercentileOrAverage.AVERAGE)
        sut.calculatePercentileAverage(listOf(50)).shouldBe(50)
        sut.calculatePercentileAverage(listOf(100, 50, 200)).shouldBe(116)
        sut.calculatePercentileAverage(listOf(100, 50, 200).sortedDescending()).shouldBe(116)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300)).shouldBe(175)
        sut.calculatePercentileAverage(listOf(50, 100, 150, 200, 250, 300).sortedDescending()).shouldBe(175)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }).shouldBe(225)
        sut.calculatePercentileAverage(IntRange(0, 9).map { it * 50 }.sortedDescending()).shouldBe(225)
    }
}
