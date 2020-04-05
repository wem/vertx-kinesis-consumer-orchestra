package ch.sourcemotion.vertx.kinesis.consumer.orchestra.api

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.awssdk.services.kinesis.model.Record
import java.time.Instant

/**
 * Jsonfieable version of [Record]. This class is designed to be enable implementations of
 * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle] to transfer AWS records
 * over the Vert.x event bus.
 */
data class JsonRecord(
    val sequenceNumber: String,
    val approximateArrivalTimestamp: Instant,
    val data: ByteArray,
    val partitionKey: String,
    val encryptionType: EncryptionType?
) {
    fun toAwsRecord() = Record.builder()
        .sequenceNumber(sequenceNumber)
        .approximateArrivalTimestamp(approximateArrivalTimestamp)
        .data(SdkBytes.fromByteArray(data))
        .partitionKey(partitionKey)
        .encryptionType(encryptionType)
        .build()
}

fun Record.toJsonRecord() = JsonRecord(
    sequenceNumber(),
    approximateArrivalTimestamp(),
    data().asByteArray(),
    partitionKey(),
    encryptionType()
)
