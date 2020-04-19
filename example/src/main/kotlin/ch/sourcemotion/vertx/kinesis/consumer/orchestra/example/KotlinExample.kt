package ch.sourcemotion.vertx.kinesis.consumer.orchestra.example

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestra
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isFalse
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.reactiverse.awssdk.VertxSdkClient
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.RedisOptions
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import mu.KLogging
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import java.net.URI
import java.util.*
import java.util.function.Supplier

/**
 * Kotlin example that should explain in a simple way how, we could consume records and dispatch them in a fan-out fashion.
 * Fan-out means we create a fair chunks of the received records and send each of those chunks over the event bus. So each
 * chunk can be proceed on event bus loop, to increase parallelism with minimal Thread context switches and therefore performance.
 */
class KotlinExample {
    companion object : KLogging() {

        private const val STREAM_NAME = "kotlin-stream"
        private const val APPLICATION_NAME = "kotlin-app"
        private const val FAN_OUT_ADDRESS = "/orchestra/fan-out"
        private const val FAN_OUT_TARGET_COUNT = 2
        private const val KINESIS_ENDPOINT = "http://localhost:4568"

        @JvmStatic
        fun main(args: Array<String>) {
            // CBOR not supported by Localstack
            System.setProperty("aws.cborEnabled", "false")
            // Register Kotlin Jackson module to enable use of Kotlin classes with Jackson
            DatabindCodec.mapper().registerKotlinModule()
            DatabindCodec.prettyMapper().registerKotlinModule()

            val awsCredentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    "test-access-key",
                    "test-secret-key"
                )
            )

            val vertx = Vertx.vertx()

            CoroutineScope(vertx.dispatcher()).launch {
                val kinesisClient = createKinesisClient(vertx, awsCredentialsProvider)
                val streamNames = kinesisClient.listStreams().await().streamNames()
                if (streamNames.contains(STREAM_NAME).isFalse()) {
                    kinesisClient.createStream { builder ->
                        builder.shardCount(1)
                        builder.streamName(STREAM_NAME)
                    }.await()
                }

                logger.info { "Kotlin example Stream created" }

                val fanOutOptions = KinesisRecordFanOutVerticleOptions(FAN_OUT_ADDRESS)

                // We deploy the fan out verticle. Each of those instances will receive a split of the bunch of records.
                // This way we proceed the records in parallel.
                vertx.deployVerticleAwait(
                    KinesisRecordFanOutVerticle::class.java.name,
                    DeploymentOptions().setInstances(FAN_OUT_TARGET_COUNT).setConfig(JsonObject.mapFrom(fanOutOptions))
                )

                logger.info { "Fan out deployed" }

                val consumerOptions = KotlinRecordConsumerOptions(FAN_OUT_ADDRESS, FAN_OUT_TARGET_COUNT)

                val orchestraOptions = VertxKinesisOrchestraOptions(
                    APPLICATION_NAME,
                    STREAM_NAME,
                    kinesisEndpoint = KINESIS_ENDPOINT,
                    credentialsProviderSupplier = Supplier { awsCredentialsProvider },
                    redisOptions = RedisOptions().setConnectionString("redis://localhost:6379"),
                    consumerVerticleClass = KotlinRecordConsumer::class.java.name,
                    consumerVerticleConfig = JsonObject.mapFrom(consumerOptions)
                )

                VertxKinesisOrchestra.create(vertx, orchestraOptions).startAwait()

                putRecords(this, kinesisClient)
            }
        }

        /**
         * We put 1'000'000 records on Kinesis
         */
        private fun putRecords(scope: CoroutineScope, kinesisClient: KinesisAsyncClient) {
            val recordBunches = 10000
            val recordsPerBunch = 100
            scope.launch {
                repeat(recordBunches) { bunchNbr ->
                    val partitionKey = UUID.randomUUID().toString()
                    logger.info { "Start to send bunch \"$bunchNbr\" of records" }
                    kinesisClient.putRecords { builder ->
                        builder.streamName(STREAM_NAME)
                        builder.records(Array(recordsPerBunch) { recordNbr ->
                            PutRecordsRequestEntry.builder()
                                .partitionKey(partitionKey)
                                .data(SdkBytes.fromUtf8String("record-${bunchNbr}-$recordNbr")).build()

                        }.toList())
                    }.await()
                }
                logger.info { "\"${recordBunches * recordsPerBunch}\" records sent" }
            }
        }

        private fun createKinesisClient(vertx: Vertx, awsCredentialsProvider: AwsCredentialsProvider): KinesisAsyncClient {
            val builder = KinesisAsyncClient.builder()
                .region(Region.EU_WEST_1)
                .credentialsProvider(awsCredentialsProvider)
                .endpointOverride(URI.create(KINESIS_ENDPOINT))
            return VertxSdkClient.withVertx(builder, vertx.orCreateContext).build()
        }
    }
}
