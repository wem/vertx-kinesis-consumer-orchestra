package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.redis.client.RedisOptions

/**
 * Basic options of [AbstractKinesisConsumerVerticle]. For internal use only, but if the user provides a configuration
 * for it's own consumer verticle implementation via [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.consumerVerticleConfig],
 * they will get merged. So both configurations must be able to ignore unknown properties as they are not aware of each other.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
internal data class KinesisConsumerVerticleOptions(
    val applicationName: String,
    val streamName: String,
    val shardIteratorStrategy: ShardIteratorStrategy,
    val errorHandling: ErrorHandling,
    val kinesisPollIntervalMillis: Long,
    val recordsPerPollLimit: Int,
    val redisOptions: RedisOptions,
    val sequenceNbrImportAddress: String? = null
)
