package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

data class RecordPutBunching(
    val recordBunches: Int,
    val recordsPerBunch: Int,
    val recordCount: Int = recordBunches * recordsPerBunch
) {
    infix fun addToCount(additionalToCount: Int) = copy(recordCount = recordCount + additionalToCount)
}

infix fun Int.bunchesOf(recordsPerBunch: Int) = RecordPutBunching(this, recordsPerBunch)
