package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.DisabledAwsClientMetricOptions
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.json.JsonObject
import io.vertx.redis.client.RedisOptions
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import java.time.Duration
import java.util.function.Supplier

data class VertxKinesisOrchestraOptions @JvmOverloads constructor(
    /**
     * The name of the application this orchestra is used by. This name is represents the discriminator between different orchestras.
     * Shard iterators and states are persisted on Redis per application, so it's possible that multiple application independently
     * consumes records from the same stream.
     */
    var applicationName: String,

    /**
     * Name of the stream this orchestra should consume records from.
     */
    var streamName: String,

    /**
     * AWS region on which the orchestra will run.
     */
    var region: String = DEFAULT_REGION.id(),

    /**
     * Interval Kinesis should be queried for new records. If the processing time of the previous received bunch of records did
     * take longer than this interval, then Kinesis will get queried immediately when this work get done.
     */
    var kinesisPollInterval: Duration = Duration.ofMillis(DEFAULT_KINESIS_POLL_INTERVAL_MILLIS),

    /**
     * The max. amount of records per query against Kinesis.
     */
    var recordsPerPollLimit: Int = DEFAULT_RECORDS_PER_POLL_LIMIT,

    /**
     * Expiration of the progress flag during shard processing. If the flag is not updated within this expiration period
     * the shard will be handled as not currently processed by any consumer. This is to avoid death locks in the case of
     * ungracefully shutdown of a consumer or the whole orchestra.
     */
    var shardProgressExpiration: Duration = Duration.ofMillis(DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS),

    /**
     * If the user has its own implementation of [ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService] this can be configured here.
     * As default [ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle]
     * will be used. To be save, please ensure to deploy the own implementation of [ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService]
     * before you start the orchestra, as this service will be used immediately.
     *
     * You must use a variant of [ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory.expose] to expose your custom shard state persistence implementation!
     */
    var useCustomShardStatePersistenceService: Boolean = false,

    /**
     * Supplier of the correct credentials for the respectively environment.
     */
    var credentialsProviderSupplier: Supplier<AwsCredentialsProvider> = Supplier { DefaultCredentialsProvider.create() },

    /**
     * Alternative kinesis endpoint.
     */
    var kinesisEndpoint: String? = null,

    /**
     * Vert.x Redis options. Used to configure Redis clients they access shard.
     */
    var redisOptions: RedisOptions,

    /**
     * AWS SDK metrics options
     */
    var awsClientMetricOptions: AwsClientMetricOptions = DisabledAwsClientMetricOptions(),

    /**
     * If the connection to Redis get lost, the orchestra will try to reconnect to Redis in this interval.
     */
    val redisReconnectionInterval: Long = DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS,

    /**
     * Strategy how and which shard iterator should be used. Please read Javadoc of
     * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy] too for more information.
     */
    var shardIteratorStrategy: ShardIteratorStrategy = ShardIteratorStrategy.EXISTING_OR_LATEST,

    /**
     * Finally the definition how many max. shards an orchestra instance could / should consume.
     * Please read also the Javadoc on [ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadStrategy] too
     * for more information.
     */
    var loadConfiguration: LoadConfiguration = LoadConfiguration.createExactConfig(1),

    /**
     * How the orchestra should behave on failures during record processing.
     * Please read Javadoc on [ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling] too for more information.
     */
    var errorHandling: ErrorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,

    /**
     * To avoid multiple consumer are processing the same shard, during deployment of them a lock will be acquired.
     * In the case of ungracefully shutdown of the whole orchestra this expiration should avoid death locks, and therefore
     * no consumer can get deployed later.
     */
    val consumerDeploymentLockExpiration: Duration = Duration.ofMillis(
        DEFAULT_CONSUMER_DEPLOYMENT_LOCK_EXPIRATION_MILLIS
    ),

    /**
     * Interval of the retry to acquire consumer deployment lock. Each orchestra instance have to get acquire the lock during
     * deployment of the consumer. So the "right" configuration here results in shorten overall
     * (over all orchestra instances) deployment time.
     */
    val consumerDeploymentLockRetryInterval: Duration = Duration.ofMillis(
        DEFAULT_CONSUMER_DEPLOYMENT_LOCK_ACQUISITION_INTERVAL_MILLIS
    ),

    /**
     * It's possible to register a event bus consumer on this address. He will get notified when the orchestra
     * did redeploy the consumer(s) because of a rehsarding.
     */
    var reshardingNotificationAddress: String = "/kinesis-consumer-orchester/resharding",

    /**
     * Class of record consumer verticle. Each consumer verticle will process one shard. This means
     * one instance per shard will get deployed and consumes records of the corresponding shard.
     *
     * Must be the full qualified class name of an implementation of:
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle]
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle]
     */
    var consumerVerticleClass: String,

    /**
     * Additional configuration, passed as options / configuration to the deployment of [consumerVerticleClass].
     *
     * IMPORTANT:
     * Be aware that this options are a combination with the internal configuration, so if you use Jackson
     * please add @[JsonIgnoreProperties] with [JsonIgnoreProperties.ignoreUnknown] true configured.
     */
    var consumerVerticleConfig: JsonObject = JsonObject(),

    /**
     * Options to import last proceeded Kinesis sequence number per shard, according to KCL v1.
     * https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-concepts
     *
     * The importer will read the most recent sequence number of a lease for a shard, so the VKCS will continue to fetch
     * from this sequence number.
     */
    var kclV1ImportOptions: KCLV1ImportOptions? = null
) {
    companion object {
        const val DEFAULT_KINESIS_POLL_INTERVAL_MILLIS = 1000L
        const val DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS = 10000L
        const val DEFAULT_REDIS_RECONNECTION_INTERVAL_MILLIS = 2000L
        const val DEFAULT_CONSUMER_DEPLOYMENT_LOCK_EXPIRATION_MILLIS = 10000L
        const val DEFAULT_CONSUMER_DEPLOYMENT_LOCK_ACQUISITION_INTERVAL_MILLIS = 500L

        // https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
        const val DEFAULT_RECORDS_PER_POLL_LIMIT = 10000
        val DEFAULT_REGION: Region = Region.EU_WEST_1
    }
}

enum class ErrorHandling {
    /**
     * In the case of failure and the consumer throws an exception of type
     * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerException], it will be retried to
     * call the consumer with a subset of the received records, starting with the failed. So, the shard iterator will be set
     * to the one of the sequence number of the failed record.
     *
     * If another exception is thrown we expect that this is an unhandled error an call the consumer with all records again.
     */
    RETRY_FROM_FAILED_RECORD,

    /**
     * Fast forward, we log and continue with next bunch of records.
     */
    IGNORE_AND_CONTINUE
}

enum class ShardIteratorStrategy {
    /**
     * If there is an existing shard iterator present (e.g. from a previous run), this iterator will be used (continued from).
     * When there is no iterator information available for the shard, the latest (queried from Kinesis) will be used.
     */
    EXISTING_OR_LATEST,

    /**
     * No check for existing iterator information. Kinesis will directly get queried for latest shard iterator, which will then
     * be used.
     */
    FORCE_LATEST
}

/**
 * Configuration how many shards and therefore consumer will be deployed per orchestra instance.
 */
enum class LoadStrategy {
    /**
     * Potential over-committing. All free shards (they are not already in progress by another orchestra instance) will get processed.
     *
     * IMPORTANT:
     * This configuration makes only sense if there is only one orchestra instance (per application) configured on a stream.
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

data class KCLV1ImportOptions @JvmOverloads constructor(
    val importAddress: String = DEFAULT_IMPORT_ADDRESS,
    val leaseTableName: String,
    val dynamoDbEndpoint: String? = null,

    /**
     * Make it possible to use DynamoDB specific aws credentials provider. If not defined,
     * [VertxKinesisOrchestraOptions.credentialsProviderSupplier] will be used instead.
     */
    val credentialsProviderSupplier: Supplier<AwsCredentialsProvider>? = null
) {
    companion object {
        const val DEFAULT_IMPORT_ADDRESS = "/kinesis-consumer-orchester/kcl-import"
    }
}
