package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import io.vertx.core.Vertx

fun Vertx.shareCredentialsProvider() {
    SharedData.shareInstance(
        this,
        ShareableAwsCredentialsProvider(Localstack.credentialsProvider),
        ShareableAwsCredentialsProvider.SHARED_DATA_REF
    )
}
