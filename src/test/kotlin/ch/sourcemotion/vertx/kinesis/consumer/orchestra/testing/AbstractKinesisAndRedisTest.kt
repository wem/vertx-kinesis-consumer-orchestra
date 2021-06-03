package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.SharedData
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.kinesis.KinesisAsyncClientFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import kotlin.LazyThreadSafetyMode.NONE


internal abstract class AbstractKinesisAndRedisTest(deployShardPersistence: Boolean = true) :
    AbstractRedisTest(deployShardPersistence) {

    protected val kinesisClient: KinesisAsyncClient by lazy(NONE) {
        SharedData.getSharedInstance<KinesisAsyncClientFactory>(vertx, KinesisAsyncClientFactory.SHARED_DATA_REF)
            .createKinesisAsyncClient(context)
    }

    @BeforeEach
    internal fun setUpKinesisTest() {
        vertx.shareCredentialsProvider()
        vertx.shareKinesisAsyncClientFactory()
    }

    @AfterEach
    internal fun cleanUpKinesisTest() = asyncBeforeOrAfter {
        cleanupKinesisStreams()
    }

    private suspend fun cleanupKinesisStreams() {
        val streamNames = kinesisClient.listStreams().await().streamNames()
        val streamToDelete = streamNames.firstOrNull { it == TEST_STREAM_NAME }
        if (streamToDelete != null) {
            kinesisClient.deleteStream { builder ->
                builder.streamName(streamToDelete).enforceConsumerDeletion(true)
            }.await()

            // Stream deletion is delayed, so we have to poll but it's faster than to restart the whole localstack
            var hasStreams: Boolean
            do {
                delay(1000)
                streamsExisting =
                    kinesisClient.listStreams().await().streamNames().any { it.startsWith(TEST_STREAM_NAME) }
            } while (streamsExisting)
            logger.info { "Kinesis streams cleaned up" }
        } else {
            logger.info { "Kinesis stream clean up not necessary" }
        }
    }
}