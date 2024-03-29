package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.credentials.ShareableAwsCredentialsProvider
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import java.util.*
import kotlin.LazyThreadSafetyMode.NONE

internal class KCLV1ImporterTest : AbstractVertxTest() {
    companion object {
        private const val IMPORTER_ADDR = "/testing/importer"
    }

    private val dynamoDbClient by lazy(NONE) {
        val builder = DynamoDbAsyncClient.builder().apply {
            credentialsProvider(AWS_CREDENTIALS_PROVIDER)
        }
        VertxSdkClient.withVertx(builder, context).build()
    }

    @BeforeEach
    internal fun setUp() = asyncBeforeOrAfter {
        shareCredentialsProviders()
        dynamoDbClient.forceCreateLeaseTable(LEASE_TABLE_NAME)
    }

    @AfterEach
    internal fun tearDown() = asyncBeforeOrAfter {
        dynamoDbClient.deleteTableIfExists(LEASE_TABLE_NAME)
    }

    @Test
    internal fun import_existing_sequence_number(testContext: VertxTestContext) = asyncTest(testContext) {
        deployImporter()

        val shardId = ShardIdGenerator.generateShardId()
        val checkpointSequenceNumber = "${UUID.randomUUID()}"
        dynamoDbClient.putLeases(LEASE_TABLE_NAME, shardId to checkpointSequenceNumber)

        val importedCheckpointSequenceNumber =
            vertx.eventBus().request<String>(IMPORTER_ADDR, "$shardId").await().body()
        importedCheckpointSequenceNumber.shouldBe(checkpointSequenceNumber)
    }

    @Test
    internal fun import_not_existing_sequence_number(testContext: VertxTestContext) = asyncTest(testContext) {
        deployImporter()

        val importedCheckpointSequenceNumber =
            vertx.eventBus().request<String>(IMPORTER_ADDR, "${ShardIdGenerator.generateShardId()}").await().body()
        importedCheckpointSequenceNumber.shouldBeNull()
    }

    @Test
    internal fun import_not_existing_table(testContext: VertxTestContext) = asyncTest(testContext) {
        shouldThrow<VertxKinesisConsumerOrchestraException> { deployImporter("not_existing_table") }
    }

    private suspend fun deployImporter(tableName: String = LEASE_TABLE_NAME) {
        vertx.deployVerticle(
            KCLV1Importer::class.java.name, deploymentOptionsOf(
                config = JsonObject.mapFrom(
                    KCLV1ImporterOptions(tableName, IMPORTER_ADDR)
                )
            )
        ).await()
    }

    private fun shareCredentialsProviders() {
        SharedData.shareInstance(
            vertx,
            ShareableAwsCredentialsProvider(AWS_CREDENTIALS_PROVIDER),
            ShareableAwsCredentialsProvider.SHARED_DATA_REF
        )

        SharedData.shareInstance(
            vertx,
            KCLV1ImporterCredentialsProvider(AWS_CREDENTIALS_PROVIDER),
            KCLV1ImporterCredentialsProvider.SHARED_DATA_REF
        )
    }
}
