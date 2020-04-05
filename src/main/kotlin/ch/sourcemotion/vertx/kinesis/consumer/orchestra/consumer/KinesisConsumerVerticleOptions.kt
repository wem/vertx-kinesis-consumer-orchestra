package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.redis.client.RedisOptions

/**
 * Basic options of [AbstractKinesisConsumerVerticle]. For internal use only in general.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
internal class KinesisConsumerVerticleOptions(
    val applicationName: String,
    val streamName: String,
    val shardIteratorStrategy: ShardIteratorStrategy,
    val errorHandling: ErrorHandling,
    val kinesisPollIntervalMillis: Long,
    val recordsPerPollLimit: Int,
    val redisOptions: RedisOptions
)
