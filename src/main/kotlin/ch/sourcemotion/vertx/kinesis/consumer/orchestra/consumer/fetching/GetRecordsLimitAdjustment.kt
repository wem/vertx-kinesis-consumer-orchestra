package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import mu.KLogging
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse

internal class GetRecordsLimitAdjustment private constructor(
    private val streamName: String,
    private val shardId: ShardId,
    initialLimit: Int,
    getRecordResultsToStartAdjustment: Int,
    private val limitIncreaseStep: Int,
    private val limitDecreaseStep: Int,
    private val limitIncreaseThreshold: Int,
    private val limitDecreaseThreshold: Int,
    private val minimalLimit: Int,
    private val percentile: RecordCountPercentileAverage
) {
    companion object : KLogging() {
        const val MAX_ALLOWED_LIMIT = 10000

        fun withOptions(streamName: String, shardId: ShardId, options: FetcherOptions) = GetRecordsLimitAdjustment(
            streamName,
            shardId,
            options.getRecordsLimit,
            options.dynamicLimitAdjustment.getRecordResultsToStartAdjustment,
            options.dynamicLimitAdjustment.limitIncreaseStep,
            options.dynamicLimitAdjustment.limitDecreaseStep,
            options.dynamicLimitAdjustment.limitIncreaseThreshold,
            options.dynamicLimitAdjustment.limitDecreaseThreshold,
            options.dynamicLimitAdjustment.minimalLimit,
            RecordCountPercentileAverage(options.dynamicLimitAdjustment.limitAdjustmentPercentileOrAverage)
        )
    }

    private var currentLimit = initialLimit
    private val recordCountList = RollingIntList(getRecordResultsToStartAdjustment)

    fun includeResponse(getRecordsResponse: GetRecordsResponse) = recordCountList.add(getRecordsResponse.records().size)

    fun calculateNextLimit(): Int {
        if (recordCountList.full) {
            val currentRecordsAverage = percentile.calculatePercentileAverage(recordCountList)
            if (currentRecordsAverage.increaseThresholdExceeded()) {
                if ((currentLimit + limitIncreaseStep) <= MAX_ALLOWED_LIMIT) {
                    currentLimit += limitIncreaseStep
                    logger.info { "Current get records limit increased to \"$currentLimit\" on stream \"$streamName\" / shard \"$shardId\"" }
                } else if (currentLimit < MAX_ALLOWED_LIMIT) {
                    currentLimit = MAX_ALLOWED_LIMIT
                    logger.info { "Current get records limit increased to allowed maximum \"$currentLimit\" on stream \"$streamName\" / shard \"$shardId\"" }
                }
            } else if (currentRecordsAverage.decreaseThresholdExceeded()) {
                if ((currentLimit - limitDecreaseStep) >= minimalLimit) {
                    currentLimit -= limitDecreaseStep
                    logger.info { "Current get records limit decreased to \"$currentLimit\" on stream \"$streamName\" / shard \"$shardId\"" }
                } else {
                    currentLimit = minimalLimit
                    logger.info { "Current get records limit decreased to configured minimum \"$minimalLimit\" on stream \"$streamName\" / shard \"$shardId\"" }
                }
            }
        }
        return currentLimit
    }

    private fun Int.increaseThresholdExceeded() = this > (currentLimit - limitIncreaseThreshold)
    private fun Int.decreaseThresholdExceeded() = this < (currentLimit - limitDecreaseThreshold)
}
