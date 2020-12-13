package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.eventbus.Message
import io.vertx.kotlin.core.eventbus.completionHandlerAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.net.URI
import kotlin.LazyThreadSafetyMode.NONE

/**
 * Importer of KCL checkpoint sequence values. This is useful to migrate from KCL v1 to VKCO.
 * This way the event processing can be continued seamless after migration.
 */
internal class KCLV1Importer : CoroutineVerticle() {
    companion object : KLogging() {
        const val CHECKPOINT_ATTR = "checkpoint"
        const val LEASE_KEY_ATTR = "leaseKey"
    }

    private val options by lazy(NONE) { config.mapTo(KCLV1ImporterOptions::class.java) }
    private val dynamoDbClient by lazy(NONE) {
        val credentialsProvider = lookupAwsCredentialsProvider()
        val builder = DynamoDbAsyncClient.builder().apply {
            val dynamoDbEndpoint = options.dynamoDbEndpoint
            if (dynamoDbEndpoint.isNotNullOrBlank()) {
                endpointOverride(URI(dynamoDbEndpoint))
            }
            credentialsProvider(credentialsProvider)
        }
        VertxSdkClient.withVertx(builder, context).build()
    }

    private fun lookupAwsCredentialsProvider(): AwsCredentialsProvider {
        val hasSpecificCredentialsProvider = SharedData.containsSharedInstanceForReference(
            vertx,
            KCLV1ImporterCredentialsProvider.SHARED_DATA_REF
        )

        return if (hasSpecificCredentialsProvider) {
            SharedData.getSharedInstance(vertx, KCLV1ImporterCredentialsProvider.SHARED_DATA_REF)
        } else {
            SharedData.getSharedInstance(vertx, ShareableAwsCredentialsProvider.SHARED_DATA_REF)
        }
    }

    override suspend fun start() {
        validateLeaseTableConfiguration()
        vertx.eventBus().localConsumer(options.importAddress, ::onImportRequest).completionHandlerAwait()
        logger.info { "KCL V1 importer started. Imports checkpoints by shard on demand" }
    }

    /**
     * Validation of the configured lease table during deployment.
     */
    private suspend fun validateLeaseTableConfiguration() {
        dynamoDbClient.describeTable { builder ->
            builder.tableName(options.leaseTableName)
        }.runCatching { await() }.getOrElse {
            throw VertxKinesisConsumerOrchestraException("KCL V1 lease table looks like configured incorrectly", it)
        }
    }

    private fun onImportRequest(msg: Message<String>) {
        val shardId = msg.body()
        launch {
            dynamoDbClient.getItem { builder ->
                builder.tableName(options.leaseTableName)
                    .key(mapOf(LEASE_KEY_ATTR to AttributeValue.builder().s(shardId).build()))
                    .attributesToGet(CHECKPOINT_ATTR)
            }.runCatching { await() }
                .onSuccess {
                    val checkpointSequenceNumberValue = it.item()[CHECKPOINT_ATTR]?.s()
                    if (checkpointSequenceNumberValue.isNotNullOrBlank()) {
                        logger.info { "Import sequence number \"$checkpointSequenceNumberValue\" from KCL V1 on lease table \"${options.leaseTableName}\" and shard id \"$shardId\"" }
                    } else {
                        logger.warn { "No sequence number available to import from KCL V1 on lease table \"${options.leaseTableName}\" and shard id \"$shardId\"" }
                    }
                    msg.reply(checkpointSequenceNumberValue)
                }.onFailure {
                    logger.error(it) { "Import of sequence number from KCL V1 on lease table \"${options.leaseTableName}\" and shard id \"$shardId\" did fail" }
                    msg.fail(0, it.message)
                }
        }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class KCLV1ImporterOptions(
    val leaseTableName: String,
    val importAddress: String,
    val dynamoDbEndpoint: String?
)
