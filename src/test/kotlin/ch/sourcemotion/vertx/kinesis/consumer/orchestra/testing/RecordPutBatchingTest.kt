package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RecordPutBatchingTest {

    @Test
    internal fun record_counts() {
        val bunch = 1 batchesOf 100
        bunch.recordBatches.shouldBe(1)
        bunch.recordsPerBatch.shouldBe(100)
        bunch.recordCount.shouldBe(100)

        val bunchWithAdditionalExpectedRecord = bunch addToCount  1
        bunchWithAdditionalExpectedRecord.recordBatches.shouldBe(1)
        bunchWithAdditionalExpectedRecord.recordsPerBatch.shouldBe(100)
        bunchWithAdditionalExpectedRecord.recordCount.shouldBe(101)
    }
}
