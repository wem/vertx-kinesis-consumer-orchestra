package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials

import io.vertx.core.shareddata.Shareable
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * [AwsCredentialsProvider] delegate which is shareable within the Vert.x local shared data.
 */
internal class ShareableAwsCredentialsProvider(private val delegate: AwsCredentialsProvider) :
    AwsCredentialsProvider by delegate, Shareable {
    companion object {
        const val SHARED_DATA_REF = "shareable-aws-credentials-provider"
    }
}
