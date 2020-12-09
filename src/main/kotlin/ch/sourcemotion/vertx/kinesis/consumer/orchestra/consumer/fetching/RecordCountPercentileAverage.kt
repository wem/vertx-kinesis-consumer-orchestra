package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.DynamicLimitAdjustmentPercentileOrAverage
import kotlin.math.ceil
import kotlin.math.floor

internal class RecordCountPercentileAverage(private val percentileOrAverageOption: DynamicLimitAdjustmentPercentileOrAverage) {

    fun calculatePercentileAverage(recordCounts: List<Int>): Int {
        val recordCountToCalcAverage = recordCounts.getCountsAccordingPercentile()
        return recordCountToCalcAverage.average().toInt()
    }

    private fun List<Int>.getCountsAccordingPercentile(): List<Int> =
        if (size <= 1 || percentileOrAverageOption == DynamicLimitAdjustmentPercentileOrAverage.AVERAGE) { // On average, all values are included
            this
        } else {
            val quantileIdxMiddle = size * percentileOrAverageOption.quantile
            val quantileIdxStart = floor(quantileIdxMiddle).toInt()
            val quantileIdxEnd = ceil(quantileIdxMiddle).toInt()
            val startIdxToTake = if (quantileIdxStart == quantileIdxEnd) quantileIdxStart - 1 else quantileIdxStart
            val endIdxToTake = if (quantileIdxEnd == quantileIdxStart) quantileIdxEnd + 1 else quantileIdxEnd
            sorted().subList(startIdxToTake, endIdxToTake)
        }
}
