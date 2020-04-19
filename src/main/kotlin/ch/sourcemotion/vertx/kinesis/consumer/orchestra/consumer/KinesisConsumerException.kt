package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.api.JsonRecord
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.api.toJsonRecord
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import software.amazon.awssdk.services.kinesis.model.Record

/**
 * Exception, that could / should be used to notify about record processing failures. If this exception is used,
 * e.g. the retry behavior can be controlled in limited manner.
 */
class KinesisConsumerException(
    val failedRecord: JsonRecord,
    message: String? = null,
    val errorHandling: ErrorHandling? = null
) : Exception(message) {
    companion object {
        const val KINESIS_CONSUMER_FAILURE_TYPE = Int.MAX_VALUE - 1
    }
}

/**
 * @return True if the failure code of this [ReplyException] is of kinesis consumer failure value
 */
fun ReplyException.isKinesisConsumerException(): Boolean =
    failureCode() == KinesisConsumerException.KINESIS_CONSUMER_FAILURE_TYPE

/**
 * Mapping of this [ReplyException] to [KinesisConsumerException]
 */
fun ReplyException.toKinesisConsumerException(): KinesisConsumerException =
    JsonObject(message).mapTo(KinesisConsumerException::class.java)

/**
 * Replies consumer failed exception and instruct orchestra to ignore and continue.
 */
fun <T> Message<T>.replyConsumeRecordFailedIgnore(
    record: Record,
    logMsg: String? = null
) {
    replyConsumeRecordFailedIgnore(record.toJsonRecord(), logMsg)
}

/**
 * Replies consumer failed exception and instruct orchestra to ignore and continue.
 */
fun <T> Message<T>.replyConsumeRecordFailedIgnore(
    record: JsonRecord,
    logMsg: String? = null
) {
    replyConsumeRecordFailed(record, ErrorHandling.IGNORE_AND_CONTINUE, logMsg)
}

/**
 * Replies consumer failed exception and instruct orchestra to retry from the given record.
 */
fun <T> Message<T>.replyConsumeRecordFailedRetry(
    record: Record,
    logMsg: String? = null
) {
    replyConsumeRecordFailedRetry(record.toJsonRecord(), logMsg)
}

/**
 * Replies consumer failed exception and instruct orchestra to retry from the given record.
 */
fun <T> Message<T>.replyConsumeRecordFailedRetry(
    record: JsonRecord,
    logMsg: String? = null
) {
    replyConsumeRecordFailed(record, ErrorHandling.RETRY_FROM_FAILED_RECORD, logMsg)
}

fun <T> Message<T>.replyConsumeRecordFailed(
    record: JsonRecord,
    errorHandling: ErrorHandling? = null,
    logMsg: String? = null
) {
    fail(
        KinesisConsumerException.KINESIS_CONSUMER_FAILURE_TYPE,
        DatabindCodec.mapper().writeValueAsString(KinesisConsumerException(record, logMsg, errorHandling))
    )
}
