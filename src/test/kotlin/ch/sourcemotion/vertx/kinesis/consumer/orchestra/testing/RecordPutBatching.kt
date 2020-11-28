package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

data class RecordPutBatching(
    val recordBatches: Int,
    val recordsPerBatch: Int,
    val recordCount: Int = recordBatches * recordsPerBatch
) {
    infix fun addToCount(additionalToCount: Int) = copy(recordCount = recordCount + additionalToCount)
}

infix fun Int.batchesOf(recordsPerBunch: Int) = RecordPutBatching(this, recordsPerBunch)
