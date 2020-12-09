package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ErrorHandling
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Basic options of [AbstractKinesisConsumerVerticle]. For internal use only, but if the user provides a configuration
 * for it's own consumer verticle implementation via [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions.consumerVerticleOptions],
 * they will get merged. So both configurations must be able to ignore unknown properties as they are not aware of each other.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
internal data class KinesisConsumerVerticleOptions(
    val shardId: ShardId,
    val clusterName: OrchestraClusterName,
    val shardIteratorStrategy: ShardIteratorStrategy,
    val errorHandling: ErrorHandling,
    val sequenceNbrImportAddress: String? = null,
    val shardProgressExpirationMillis : Long,
    val fetcherOptions: FetcherOptions
)
