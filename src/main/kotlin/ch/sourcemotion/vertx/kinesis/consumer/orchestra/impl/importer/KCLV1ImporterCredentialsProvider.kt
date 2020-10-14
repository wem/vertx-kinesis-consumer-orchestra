package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer

import io.vertx.core.shareddata.Shareable
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * [AwsCredentialsProvider] delegate which is shareable within the Vert.x local shared data.
 *
 * This [AwsCredentialsProvider] will be used by [KCLV1Importer] if supplier is defined in
 * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.KCLV1ImportOptions.credentialsProviderSupplier]
 */
internal class KCLV1ImporterCredentialsProvider(private val delegate: AwsCredentialsProvider) :
    AwsCredentialsProvider by delegate,
    Shareable {
    companion object {
        const val SHARED_DATA_REF = "kclv1-importer-aws-credentials-provider"
    }
}
