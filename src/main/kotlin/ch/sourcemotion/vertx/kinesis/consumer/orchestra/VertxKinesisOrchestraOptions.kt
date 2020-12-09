package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_ENABLED
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_ADJUSTMENT_STEP
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_DECREASE_ADJUSTMENT_THRESHOLD
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_LIMIT_INCREASE_ADJUSTMENT_THRESHOLD
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_GET_RECORDS_RESULTS_ADJUSTMENT_INCLUSION
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_LIMIT_ADJUSTMENT_PERCENTILE
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_MINIMAL_GET_RECORDS_LIMIT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_NOT_CONSUMED_SHARD_DETECTION_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_RECORDS_FETCH_INTERVAL_MILLIS
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.Companion.DEFAULT_RECORDS_PREFETCH_LIMIT
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.AwsClientMetricOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.metrics.factory.DisabledAwsClientMetricOptions
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.json.JsonObject
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
     * Vert.x Redis options. Used for shard state persistence (sequence number position of consuming shard) and
     * VKCO cluster communication (if not Vert.x is used)
     */
    var redisOptions: RedisHeimdallOptions,

    /**
     * AWS SDK metrics options
     */
    var awsClientMetricOptions: AwsClientMetricOptions = DisabledAwsClientMetricOptions(),

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
    var loadConfiguration: LoadConfiguration = LoadConfiguration.createConsumeExact(1),

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
     * Class of record consumer verticle. Each consumer verticle will process one shard. This means
     * one instance per shard will get deployed and consumes records of the corresponding shard.
     *
     * Must be the full qualified class name of an implementation of:
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle]
     * - [com.gardena.smartgarden.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle]
     */
    var consumerVerticleClass: String,

    /**
     * Additional configuration, passed as options to the deployment of the provided [consumerVerticleClass].
     *
     * IMPORTANT:
     * Be aware that this options are a combination with the internal configuration, so if you use Jackson
     * please add @[JsonIgnoreProperties] with [JsonIgnoreProperties.ignoreUnknown] true configured.
     */
    var consumerVerticleOptions: JsonObject = JsonObject(),

    /**
     * Options to import last proceeded Kinesis sequence number per shard, according to KCL v1.
     * https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-concepts
     *
     * The importer will read the most recent sequence number of a lease for a shard, so the VKCS will continue to fetch
     * from this sequence number.
     */
    var kclV1ImportOptions: KCLV1ImportOptions? = null,

    /**
     * Fetcher options. Default a dynamic fetcher is enabled, which will adjust the get records request limit according
     * to the count of records per response of previous requests.
     */
    val fetcherOptions: FetcherOptions = FetcherOptions()
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
        const val DEFAULT_CONSUMER_DEPLOYMENT_LOCK_EXPIRATION_MILLIS = 10000L
        const val DEFAULT_CONSUMER_DEPLOYMENT_LOCK_ACQUISITION_INTERVAL_MILLIS = 500L
        const val DEFAULT_NOT_CONSUMED_SHARD_DETECTION_INTERVAL_MILLIS = 2000L

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

data class LoadConfiguration constructor(
    /**
     * The exactly max count of shard will be processed, unaware of available event loop threads.
     * This configuration encompass the scenario where we would fan-out on consumer level, where Kinesis is not
     * the limiting factor.
     */
    val maxShardsCount: Int,

    /**
     * Interval to execute detection of not consumed shards. Common situations of not consumed shards are e.g. on
     * VKCO startup or after a split resharding.
     */
    val notConsumedShardDetectionInterval: Long
) {
    companion object {
        @JvmOverloads
        @JvmStatic
        fun createConsumeAllShards(notConsumedShardDetectionInterval: Long = DEFAULT_NOT_CONSUMED_SHARD_DETECTION_INTERVAL_MILLIS) =
            LoadConfiguration(Int.MAX_VALUE, notConsumedShardDetectionInterval)

        @JvmStatic
        fun createConsumeExact(
            exactCount: Int,
            notConsumedShardDetectionInterval: Long = DEFAULT_NOT_CONSUMED_SHARD_DETECTION_INTERVAL_MILLIS
        ) = LoadConfiguration(exactCount, notConsumedShardDetectionInterval)
    }

    init {
        if (maxShardsCount < 1) {
            throw VertxKinesisConsumerOrchestraException("Please configure a load configuration with at least 1 maxShardsCount")
        }
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
    val dynamicLimitAdjustment: DynamicLimitAdjustment = DynamicLimitAdjustment()
) {
    init {
        require(recordsFetchIntervalMillis > 0) { "recordsFetchInterval must be a positive duration. But is \"$recordsFetchIntervalMillis\"" }
        require(recordsPreFetchLimit.isPositive()) { "recordsPreFetchLimit must be a positive integer. But is \"$recordsPreFetchLimit\"" }
        require(getRecordsLimit.isPositive()) { "getRecordsLimit must be a positive integer. But is \"$getRecordsLimit\"" }
    }
}

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

private fun Int.isPositive() = this > 0

enum class DynamicLimitAdjustmentPercentileOrAverage(val quantile: Double) {
    P50(0.5), P80(0.8), P90(0.9), AVERAGE(0.0)
}
