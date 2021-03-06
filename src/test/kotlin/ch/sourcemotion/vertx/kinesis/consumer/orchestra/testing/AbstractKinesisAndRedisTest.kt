package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.future.await
import org.junit.jupiter.api.BeforeEach
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import kotlin.LazyThreadSafetyMode.NONE


internal abstract class AbstractKinesisAndRedisTest(deployShardPersistence: Boolean = true) :
    AbstractRedisTest(deployShardPersistence), LocalstackContainerTest {

    protected val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    @BeforeEach
    fun credentialsProviderKinesisClientFactoryAndShardPersistence(testContext: VertxTestContext) =
        asyncTest(testContext) {
            vertx.shareCredentialsProvider()
            vertx.shareKinesisAsyncClientFactory(getKinesisEndpointOverride())
        }

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
            var hasStreams: Boolean
            do {
                hasStreams = kinesisClient.listStreams().await().streamNames().isNotEmpty()
            } while (hasStreams)
            logger.info { "Kinesis streams cleaned up" }
        } else {
            logger.info { "Kinesis stream clean up not necessary" }
        }
    }
}
