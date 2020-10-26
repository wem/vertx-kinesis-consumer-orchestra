package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.deploymentOptionsOf
import kotlinx.coroutines.future.await
import mu.KLogging
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.junit.jupiter.Container
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient


internal abstract class AbstractKinesisAndRedisTest(private val deployShardPersistence: Boolean = true) :
    AbstractRedisTest() {

    companion object : KLogging() {
        @JvmStatic
        @Container
        var localStackContainer: LocalStackContainer = LocalStackContainer(Localstack.VERSION)
            .withServices(LocalStackContainer.Service.KINESIS)
    }

    protected val kinesisClient: KinesisAsyncClient by lazy {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    protected val shardStatePersistenceService: ShardStatePersistenceServiceAsync by lazy {
        ShardStatePersistenceServiceFactory.createAsyncShardStatePersistenceService(vertx)
    }

    @BeforeEach
    fun credentialsProviderKinesisClientFactoryAndShardPersistence(testContext: VertxTestContext) =
        asyncTest(testContext) {
            vertx.shareCredentialsProvider()
            vertx.shareKinesisAsyncClientFactory(localStackContainer.getKinesisEndpointOverride())
            if (deployShardPersistence) {
                deployShardStatePersistenceService()
            }
        }

    /**
     * Cleanup Kinesis streams before each test function as the Kinesis instance is per the class.
     */
    @BeforeEach
    fun cleanupKinesisStreams(testContext: VertxTestContext) = asyncTest(testContext) {
        val streamNames = kinesisClient.listStreams().await().streamNames()
        if (streamNames.isNotEmpty()) {
            streamNames.forEach { streamName ->
                kinesisClient.deleteStream { builder ->
                    builder.streamName(streamName)
                }.await()
            }

            // Stream deletion is delayed, so we have to poll but it's faster than to restart the whole localstack
            var streamsAfterDeletion = kinesisClient.listStreams().await()
            while (streamsAfterDeletion.streamNames().isNotEmpty()) {
                streamsAfterDeletion = kinesisClient.listStreams().await()
            }
            logger.info { "Kinesis streams cleaned up" }
        } else {
            logger.info { "Kinesis stream clean up not necessary" }
        }
    }


    private suspend fun deployShardStatePersistenceService() {
        val options = RedisShardStatePersistenceServiceVerticleOptions(
            TEST_APPLICATION_NAME,
            TEST_STREAM_NAME,
            redisOptions,
            VertxKinesisOrchestraOptions.DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS
        )
        vertx.deployVerticleAwait(
            RedisShardStatePersistenceServiceVerticle::class.java.name, deploymentOptionsOf(JsonObject.mapFrom(options))
        )
    }
}
