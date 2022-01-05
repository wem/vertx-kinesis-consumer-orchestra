package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_BALANCING_COMMAND_TIMEOUT_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_BALANCING_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_CONSUMER_ACTIVE_CHECK_INTERVAL
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_CONSUMER_REGISTRATION_RETRY_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_FETCHER_METRICS_ENABLED
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_ENABLED
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_STEP
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_DECREASE_ADJUSTMENT_THRESHOLD
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_INCREASE_ADJUSTMENT_THRESHOLD
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_RESULTS_ADJUSTMENT_INCLUSION
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_INITIAL_BALANCING_DELAY_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_LIMIT_ADJUSTMENT_PERCENTILE
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_MINIMAL_GET_RECORDS_LIMIT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_MIN_RESUBSCRIBE_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_NODE_KEEP_ALIVE_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_RECORDS_FETCH_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_RECORDS_PREFETCH_LIMIT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_USE_SDK_NETTY_CLIENT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.DisabledAwsClientMetricOptions
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.json.JsonObject
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

data class VertxKinesisOrchestraOptions @JvmOverloads constructor(
    /**
     * Identification of a single orchestra node instance. It's optional to set this by client.
     */
    val nodeId: String? = null,
    /**
     * The name of the application this orchestra is used by. This name is represents the discriminator between different orchestras.
     * Shard iterators and states are persisted on Redis per application, so it's possible that multiple application independently
     * consumes records from the same stream.
     */
    val applicationName: String,

    /**
     * Name of the stream this orchestra should consume records from.
     */
    val streamName: String,

    /**
     * AWS region on which the orchestra will run.
     */
    val region: String = DEFAULT_REGION.id(),

    /**
     * Expiration of the progress flag during shard processing. If the flag is not updated within this expiration period
     * the shard will be handled as not currently processed by any consumer. This is to avoid death locks in the case of
     * ungracefully shutdown of a consumer or the whole orchestra.
     */
    val shardProgressExpiration: Duration = Duration.ofMillis(DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS),

    /**
     * If the user has its own implementation of [ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService] this can be configured here.
     * As default [ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard.persistence.RedisShardStatePersistenceServiceVerticle]
     * will be used. To be save, please ensure to deploy the own implementation of [ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService]
     * before you start the orchestra, as this service will be used immediately.
     *
     * You must use a variant of [ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceFactory.expose] to expose your custom shard state persistence implementation!
     */
    val useCustomShardStatePersistenceService: Boolean = false,

    /**
     * Supplier of the correct credentials for the respectively environment.
     */
    val credentialsProviderSupplier: Supplier<AwsCredentialsProvider> = Supplier { DefaultCredentialsProvider.create() },

    /**
     * Options for Kinesis client to use. For all operations (except enhanced fanout) Vert.x Http client will be used.
     * To enable AWS SDK built-in Netty client, please set [FetcherOptions.useSdkNettyClient] to true.
     */
    val kinesisClientOptions: KinesisClientOptions = KinesisClientOptions(),

    /**
     * Vert.x Redis options. Used for shard state persistence (sequence number position of consuming shard) and
     * VKCO cluster communication (if not Vert.x is used)
     */
    val redisOptions: RedisHeimdallOptions,

    /**
     * AWS SDK metrics options
     */
    val awsClientMetricOptions: AwsClientMetricOptions = DisabledAwsClientMetricOptions(),

    /**
     * Strategy how and which shard iterator should be used. Please read Javadoc of
     * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy] too for more information.
     */
    val shardIteratorStrategy: ShardIteratorStrategy = ShardIteratorStrategy.EXISTING_OR_LATEST,

    /**
     * How the orchestra should behave on failures during record processing.
     * Please read Javadoc on [ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling] too for more information.
     */
    val errorHandling: ErrorHandling = ErrorHandling.RETRY_FROM_FAILED_RECORD,

    /**
     * To avoid multiple consumer are processing the same shard, during detection of them a red lock will be acquired.
     * Eg.g in the case of ungracefully shutdown of the whole orchestra this expiration should avoid death locks, and therefore
     * no consumer can get deployed later.
     */
    val detectionLockExpiration: Duration = Duration.ofMillis(
        DEFAULT_DETECTION_LOCK_EXPIRATION_MILLIS
    ),

    /**
     * Interval of the retry to acquire detection lock. Each orchestra instance have to get acquire the lock during
     * detection of consumable shards.
     */
    val detectionLockAcquisitionInterval: Duration = Duration.ofMillis(
        DEFAULT_DETECTION_LOCK_ACQUISITION_INTERVAL_MILLIS
    ),

    /**
     * Max time(out) a deployment of a shard consumers can take, before its handled as failed. Later detections
     * can retry it.
     */
    val consumerDeploymentTimeout: Duration = Duration.ofMillis(DeliveryOptions.DEFAULT_TIMEOUT),

    /**
     * Class of record consumer verticle. Each consumer verticle will process one shard. This means
     * one instance per shard will get deployed and consumes records of the corresponding shard.
     *
     * Must be the full qualified class name of an implementation of:
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle]
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle]
     */
    val consumerVerticleClass: String,

    /**
     * Additional configuration, passed as options to the deployment of the provided [consumerVerticleClass].
     *
     * IMPORTANT:
     * Be aware that this options are a combination with the internal configuration, so if you use Jackson
     * please add @[JsonIgnoreProperties] with [JsonIgnoreProperties.ignoreUnknown] true configured.
     */
    val consumerVerticleOptions: JsonObject = JsonObject(),

    /**
     * Options to import last proceeded Kinesis sequence number per shard, according to KCL v1.
     * https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-concepts
     *
     * The importer will read the most recent sequence number of a lease for a shard, so the VKCS will continue to fetch
     * from this sequence number.
     */
    val kclV1ImportOptions: KCLV1ImportOptions? = null,

    /**
     * Fetcher options. Default a dynamic fetcher is enabled, which will adjust the get records request limit according
     * to the count of records per response of previous requests.
     */
    val fetcherOptions: FetcherOptions = FetcherOptions(),

    val balancing: Balancing = Balancing()
) {
    companion object {
        /**
         * Kinesis allows 5 TX per second.
         */
        const val DEFAULT_RECORDS_FETCH_INTERVAL_MILLIS = 250L
        const val DEFAULT_RECORDS_PREFETCH_LIMIT = 10000
        const val DEFAULT_GET_RECORDS_LIMIT = 1000
        const val DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_ENABLED = true
        const val DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_STEP = 100
        val DEFAULT_LIMIT_ADJUSTMENT_PERCENTILE = DynamicLimitAdjustmentPercentileOrAverage.AVERAGE
        const val DEFAULT_GET_RECORDS_LIMIT_INCREASE_ADJUSTMENT_THRESHOLD = 300
        const val DEFAULT_GET_RECORDS_LIMIT_DECREASE_ADJUSTMENT_THRESHOLD = 700
        const val DEFAULT_GET_RECORDS_RESULTS_ADJUSTMENT_INCLUSION = 10
        const val DEFAULT_MINIMAL_GET_RECORDS_LIMIT = 300

        const val DEFAULT_SHARD_PROGRESS_EXPIRATION_MILLIS = 10000L
        const val DEFAULT_DETECTION_LOCK_EXPIRATION_MILLIS = 10000L
        const val DEFAULT_DETECTION_LOCK_ACQUISITION_INTERVAL_MILLIS = 500L

        // Balancing
        const val DEFAULT_NODE_KEEP_ALIVE_MILLIS = 3000L
        const val DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS = 3000L
        const val DEFAULT_INITIAL_BALANCING_DELAY_MILLIS = 1000L
        const val DEFAULT_BALANCING_INTERVAL_MILLIS = 2000L
        const val DEFAULT_BALANCING_COMMAND_TIMEOUT_MILLIS = 30000L // Default event bus timeout

        const val DEFAULT_USE_SDK_NETTY_CLIENT = true

        const val DEFAULT_MIN_RESUBSCRIBE_INTERVAL_MILLIS = 6000L
        const val DEFAULT_CONSUMER_REGISTRATION_RETRY_INTERVAL_MILLIS = 2000L
        const val DEFAULT_CONSUMER_ACTIVE_CHECK_INTERVAL = 500L
        const val DEFAULT_FETCHER_METRICS_ENABLED = false

        val DEFAULT_REGION: Region = Region.EU_WEST_1
        val DEFAULT_KINESIS_HTTP_CLIENT_OPTIONS: HttpClientOptions = HttpClientOptions().setSsl(true)
            .setKeepAlive(true)
            .setKeepAliveTimeout(30) // https://github.com/reactiverse/aws-sdk/issues/40
            .setIdleTimeout(5).setIdleTimeoutUnit(TimeUnit.SECONDS)
            .setConnectTimeout(10000)
    }
}

data class KinesisClientOptions(
    /**
     * Alternative kinesis endpoint.
     */
    val kinesisEndpoint: String? = null,

    /**
     * Options for the http client to access Kinesis.
     * Default as defined by io.reactiverse.awssdk.VertxNioAsyncHttpClient,
     * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient and software.amazon.awssdk.http.SdkHttpConfigurationOption
     *
     * https://github.com/reactiverse/aws-sdk/issues/40
     */
    val kinesisHttpClientOptions: HttpClientOptions = VertxKinesisOrchestraOptions.DEFAULT_KINESIS_HTTP_CLIENT_OPTIONS,
)

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
        const val DEFAULT_IMPORT_ADDRESS = "/kinesis-consumer-orchestra/kcl-import"
    }
}

data class FetcherOptions(

    /**
     * Interval to fetch records from Kinesis. Default is -1 which means, getRecords request will get executed as fast / often
     * as possible.
     */
    val recordsFetchIntervalMillis: Long = DEFAULT_RECORDS_FETCH_INTERVAL_MILLIS,

    /**
     * The fetcher will pre-fetch records and hold them in a stream. So if the fetcher is "faster" than the consumer, the
     * consumer will never have to wait for next records to proceed. But to avoid the fetcher will went too far
     * into the "future" he will get suspended if this limit is reached / exceeded.
     *
     * The consumer will get all pre-fetched at once from the stream.
     * So the next time after the consumer will read records from the stream, the fetcher will resume again.
     */
    val recordsPreFetchLimit: Int = DEFAULT_RECORDS_PREFETCH_LIMIT,

    /**
     * Applied to [software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest.limit]
     */
    val getRecordsLimit: Int = DEFAULT_GET_RECORDS_LIMIT,

    val dynamicLimitAdjustment: DynamicLimitAdjustment = DynamicLimitAdjustment(),

    /**
     * Enables metric which will count the number of fetched records
     */
    val metricsEnabled: Boolean = DEFAULT_FETCHER_METRICS_ENABLED,

    /**
     * Explicit name of the Micrometer registry to use. If null [BackendRegistries.getDefaultNow()] will be used.
     */
    val metricRegistryName: String? = null,

    /**
     * Name of the Micrometer counter that will count the number of received records.
     */
    val metricName: String? = null,

    /**
     * Optional options to use enhanced fan out. If this options are set, all other options of [FetcherOptions] are not
     * considered except [recordsPreFetchLimit].
     */
    val enhancedFanOut: EnhancedFanOutOptions? = null
) {
    init {
        require(recordsFetchIntervalMillis > 0) { "recordsFetchInterval must be a positive duration. But is \"$recordsFetchIntervalMillis\"" }
        require(recordsPreFetchLimit.isPositive()) { "recordsPreFetchLimit must be a positive integer. But is \"$recordsPreFetchLimit\"" }
        require(getRecordsLimit.isPositive()) { "getRecordsLimit must be a positive integer. But is \"$getRecordsLimit\"" }
    }
}

data class EnhancedFanOutOptions(
    val streamArn: String,

    /**
     * Enhanced fan out subscription can only happen each 5 second per consumer per shard. Some times Kinesis will respond
     * failures a short time after 5 seconds. So this value is customizable.
     */
    val minResubscribeIntervalMillis: Long = DEFAULT_MIN_RESUBSCRIBE_INTERVAL_MILLIS,

    /**
     * Retry interval for register consumer / list consumers if the requests did fail because of exceed limit or
     * resource is in use.
     */
    val consumerRegistrationRetryIntervalMillis: Long = DEFAULT_CONSUMER_REGISTRATION_RETRY_INTERVAL_MILLIS,

    /**
     * Interval to check if the consumer is in active state ([ConsumerStatus.ACTIVE]) and is ready for subscription. As an enhanced fan out consumer
     * is a common AWS resource. It has the same transitions as some other resources
     */
    val consumerActiveCheckInterval: Long = DEFAULT_CONSUMER_ACTIVE_CHECK_INTERVAL,

    /**
     * If this flag is true, the standard Netty client, shipped with the AWS SDK will be used instead of the Vert.x
     * variant.
     */
    val useSdkNettyClient: Boolean = DEFAULT_USE_SDK_NETTY_CLIENT,

    /**
     * AWS SDK Netty client configuration for max concurrency.
     * @see [software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder.maxConcurrency]
     */
    val sdkNettyMaxConcurrency: Int? = null,

    /**
     * AWS SDK Netty client configuration for max stream.
     * @see [software.amazon.awssdk.http.nio.netty.Http2Configuration.Builder.maxStreams]
     */
    val sdkNettyMaxStreams: Long? = null
)

data class DynamicLimitAdjustment(

    /**
     * If true the dynamic get record request limit is enabled.
     */
    val enabled: Boolean = DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_ENABLED,

    /**
     * After this count of [software.amazon.awssdk.services.kinesis.KinesisAsyncClient.getRecords] calls, the limit adjustment
     * will be activated.
     */
    val getRecordResultsToStartAdjustment: Int = DEFAULT_GET_RECORDS_RESULTS_ADJUSTMENT_INCLUSION,

    /**
     * If this dynamic limit adjustment detects a too low limit, the limit will increased by this value for the subsequent
     * [software.amazon.awssdk.services.kinesis.KinesisAsyncClient.getRecords] calls.
     */
    val limitIncreaseStep: Int = DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_STEP,

    /**
     * If this dynamic limit adjustment detects a too high limit, the limit will decreased by this value for the subsequent
     * [software.amazon.awssdk.services.kinesis.KinesisAsyncClient.getRecords] calls.
     */
    val limitDecreaseStep: Int = DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_STEP,

    /**
     * The method / calculation to determine the current consumed amount of records according to [getRecordResultsToStartAdjustment]
     * The result of this calculation is tightly coupled to the thresholds [limitIncreaseThreshold] and [limitDecreaseThreshold].
     */
    val limitAdjustmentPercentileOrAverage: DynamicLimitAdjustmentPercentileOrAverage = DEFAULT_LIMIT_ADJUSTMENT_PERCENTILE,

    /**
     * Threshold to detect a too low limit of get record requests according to [limitAdjustmentPercentileOrAverage].
     * If this threshold did exceed, the next get record requests limit will increased by [limitIncreaseStep].
     */
    val limitIncreaseThreshold: Int = DEFAULT_GET_RECORDS_LIMIT_INCREASE_ADJUSTMENT_THRESHOLD,

    /**
     * Threshold to detect a too high limit of get record requests according to [limitAdjustmentPercentileOrAverage].
     * If this threshold did exceed, the next get record requests limit will decreased by [limitIncreaseStep].
     */
    val limitDecreaseThreshold: Int = DEFAULT_GET_RECORDS_LIMIT_DECREASE_ADJUSTMENT_THRESHOLD,

    /**
     * Minimum of limit a get records request will get parametrized with. Unaware if the [limitDecreaseThreshold] is exceeded.
     */
    val minimalLimit: Int = DEFAULT_MINIMAL_GET_RECORDS_LIMIT,
) {
    init {
        require(getRecordResultsToStartAdjustment.isPositive()) { "getRecordResultsToStartAdjustment must be a positive integer. But is \"$getRecordResultsToStartAdjustment\"" }
        require(limitIncreaseStep.isPositive()) { "limitIncreaseStep must be a positive integer. But is \"$limitIncreaseStep\"" }
        require(limitDecreaseStep.isPositive()) { "limitIncreaseStep must be a positive integer. But is \"$limitDecreaseStep\"" }
        require(limitIncreaseThreshold.isPositive()) { "limitIncreaseThreshold must be a positive integer. But is \"$limitIncreaseThreshold\"" }
        require(limitDecreaseThreshold.isPositive()) { "limitDecreaseThreshold must be a positive integer. But is \"$limitDecreaseThreshold\"" }
        require(minimalLimit.isPositive()) { "minimalLimit must be a positive integer. But is \"$minimalLimit\"" }
    }

}

data class Balancing(
    val nodeKeepAliveMillis: Long = DEFAULT_NODE_KEEP_ALIVE_MILLIS,
    val activeBalancerCheckIntervalMillis: Long = DEFAULT_ACTIVE_BALANCER_CHECK_INTERVAL_MILLIS,
    val initialBalancingDelayMillis: Long = DEFAULT_INITIAL_BALANCING_DELAY_MILLIS,
    val balancingIntervalMillis: Long = DEFAULT_BALANCING_INTERVAL_MILLIS,
    val balancingCommandTimeoutMillis: Long = DEFAULT_BALANCING_COMMAND_TIMEOUT_MILLIS,
)

private fun Int.isPositive() = this > 0

enum class DynamicLimitAdjustmentPercentileOrAverage(val quantile: Double) {
    P50(0.5), P80(0.8), P90(0.9), AVERAGE(0.0)
}
