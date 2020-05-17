package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RecordPutBunchingTest {

    @Test
    internal fun record_counts() {
        val bunch = 1 bunchesOf 100
        bunch.recordBunches.shouldBe(1)
        bunch.recordsPerBunch.shouldBe(100)
        bunch.recordCount.shouldBe(100)

        val bunchWithAdditionalExpectedRecord = bunch addToCount  1
        bunchWithAdditionalExpectedRecord.recordBunches.shouldBe(1)
        bunchWithAdditionalExpectedRecord.recordsPerBunch.shouldBe(100)
        bunchWithAdditionalExpectedRecord.recordCount.shouldBe(101)
    }
}
