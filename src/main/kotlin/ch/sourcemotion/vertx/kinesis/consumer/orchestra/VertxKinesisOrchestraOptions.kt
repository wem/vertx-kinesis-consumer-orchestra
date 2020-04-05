package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestrationVerticleOptions
import io.vertx.core.json.JsonObject
import io.vertx.redis.client.RedisOptions
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import java.time.Duration
import java.util.function.Supplier

data class VertxKinesisOrchestraOptions @JvmOverloads constructor(
    var applicationName: String,
    var streamName: String,
    var region: String = DEFAULT_REGION.id(),
    var kinesisPollInterval: Duration = Duration.ofMillis(DEFAULT_KINESIS_POLL_INTERVAL),
    var recordsPerPollLimit: Int = DEFAULT_RECORDS_PER_POLL_LIMIT,
    /**
     * Expiration of the progress flag during shard processing. If the flag is not updated within this expiration period
     * the shard will be handled as not currently processed by any consumer. This is to avoid death locks in the case of
     * ungracefully shutdown of a consumer.
     */
    var shardProgressExpiration: Duration = Duration.ofMillis(DEFAULT_SHARD_PROGRESS_EXPIRATION),
    var credentialsProviderSupplier: Supplier<AwsCredentialsProvider> = Supplier { DefaultCredentialsProvider.create() },
    /**
     * Alternative kinesis endpoint.
     */
    var kinesisEndpoint: String? = null,

    /**
     * Original Vert.x Redis options. Used to configure Redis clients they access shard
     */
    var redisOptions: RedisOptions,

    var shardIteratorStrategy: ShardIteratorStrategy = ShardIteratorStrategy.EXISTING_OR_LATEST,
    var loadConfiguration: LoadConfiguration = LoadConfiguration.createExactConfig(1),
    var errorHandling: ErrorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,

    /**
     * To avoid multiple consumer are processing the same shard, during deployment of them a lock will be acquired.
     * In the case of ungracefully shutdown of the whole orchestra this expiration should avoid death locks, and therefore
     * no consumer can get deployed later.
     */
    val consumerDeploymentLockExpiration: Duration = Duration.ofMillis(DEFAULT_CONSUMER_DEPLOYMENT_LOCK_EXPIRATION),
    /**
     * Interval of the retry to acquire consumer deployment lock. This comes into play of very short reshardings behind each other.
     */
    val consumerDeploymentLockRetryInterval: Duration = Duration.ofMillis(
        DEFAULT_CONSUMER_DEPLOYMENT_LOCK_ACQUISITION_INTERVAL
    ),
    /**
     * It's possible to register a event bus consumer on this address. He will get notified when the orchestration
     * is adjusted for a resharding and will receive a JsonObject with the data of
     * [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.impl.resharding.ReshardingInformation]
     */
    var reshardingNotificationAddress: String = "/kinesis-consumer-orchester/resharding",
    /**
     * Must be the full qualified class name of an implementation of:
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle]
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle]
     */
    var consumerVerticleClass: String,
    /**
     * Additional configuration. They will be added to the consumer verticle ([errorHandling]) config.
     */
    var consumerVerticleConfig: JsonObject = JsonObject()
) {
    companion object {
        const val DEFAULT_KINESIS_POLL_INTERVAL = 1000L
        const val DEFAULT_SHARD_PROGRESS_EXPIRATION = 10000L
        const val DEFAULT_CONSUMER_DEPLOYMENT_LOCK_EXPIRATION = 10000L
        const val DEFAULT_CONSUMER_DEPLOYMENT_LOCK_ACQUISITION_INTERVAL = 500L

        // https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
        const val DEFAULT_RECORDS_PER_POLL_LIMIT = 10000
        val DEFAULT_REGION: Region = Region.EU_WEST_1
    }

    internal fun asOrchestraVerticleOptions() = OrchestrationVerticleOptions(
        applicationName,
        streamName,
        kinesisPollInterval.toMillis(),
        recordsPerPollLimit,
        redisOptions,
        shardIteratorStrategy,
        loadConfiguration,
        errorHandling,
        consumerDeploymentLockExpiration.toMillis(),
        consumerDeploymentLockRetryInterval.toMillis(),
        reshardingNotificationAddress,
        consumerVerticleClass,
        consumerVerticleConfig.map
    )
}

/**
 * Configures the handling of errors.
 */
enum class ErrorHandling {
    /**
     * In the case of failure and the consumer throws an exception of type
     * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerException], it will be retried to
     * call the consumer with a subset of the received records, starting with the failed. If another exception is thrown
     * we expect that this is an unhandled error an call the consumer with all records again.
     */
    RETRY_FROM_FAILED_RECORD,

    /**
     * Fast forward, we log and continue with next bulk of records.
     */
    IGNORE_AND_CONTINUE
}

enum class ShardIteratorStrategy {
    EXISTING_OR_LATEST,
    FORCE_LATEST
}

/**
 * Configuration how many shards and therefore consumer will be deployed.
 */
enum class LoadStrategy {
    /**
     * Potential over-committing. All free shards (they are not already in progress by another orchstrator instance) will get processed.
     *
     * IMPORTANT:
     * This configuration makes only sense if there is only one orchestrator instance (application) configured on a stream.
     * As the first instance will take all shards.
     */
    DO_ALL_SHARDS,

    /**
     * The exactly count of shard will be processed, unaware of available event loop threads.
     * This configuration encompass the scenario where we would fan-out on consumer level, where Kinesis is not
     * the limiting factor whats the case most times.
     */
    EXACT
}

data class LoadConfiguration(val strategy: LoadStrategy, val exactCount: Int? = null) {
    companion object {
        fun createDoAllShardsConfig() = LoadConfiguration(LoadStrategy.DO_ALL_SHARDS)
        fun createExactConfig(exactCount: Int) = LoadConfiguration(LoadStrategy.EXACT, exactCount)
    }
}
