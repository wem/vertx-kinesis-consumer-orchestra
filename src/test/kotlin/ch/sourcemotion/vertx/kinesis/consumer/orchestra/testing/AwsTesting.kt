package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import io.vertx.core.Vertx
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import kotlin.LazyThreadSafetyMode.NONE

val AWS_REGION: String by lazy(NONE) { System.getenv("AWS_REGION") }
val AWS_PROFILE: String by lazy(NONE) { System.getenv("AWS_PROFILE") }
val AWS_CREDENTIALS_PROVIDER: ProfileCredentialsProvider by lazy(NONE) { ProfileCredentialsProvider.create(AWS_PROFILE) }

fun Vertx.shareCredentialsProvider() {
    SharedData.shareInstance(
        this,
        ShareableAwsCredentialsProvider(AWS_CREDENTIALS_PROVIDER),
        ShareableAwsCredentialsProvider.SHARED_DATA_REF
    )
}
