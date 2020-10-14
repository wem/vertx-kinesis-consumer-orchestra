package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.*
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNull
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceServiceAsync
import io.vertx.core.Vertx
import io.vertx.kotlin.core.eventbus.requestAwait
import mu.KLogging
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType

internal class StartFetchPositionLookup(
    private val vertx: Vertx,
    private val consumerInfo: String,
    private val shardId: ShardId,
    private val options: KinesisConsumerVerticleOptions,
    private val shardStatePersistenceService: ShardStatePersistenceServiceAsync,
    private val kinesisClient: KinesisAsyncClient
) {
    private companion object : KLogging()

    suspend fun getStartFetchPosition(): FetchPosition {
        return when (options.shardIteratorStrategy) {
            ShardIteratorStrategy.FORCE_LATEST -> {
                logger.debug { "Force ${ShardIteratorType.LATEST.name} shard iterator on $consumerInfo" }
                FetchPosition(kinesisClient.getLatestShardIteratorAwait(options.streamName, shardId), null)
            }
            ShardIteratorStrategy.EXISTING_OR_LATEST -> {
                val existingSequenceNumber = shardStatePersistenceService.getConsumerShardSequenceNumber(shardId)
                if (existingSequenceNumber.isNotNull()) {
                    logger.debug { "Use existing shard sequence number: \"$existingSequenceNumber\" for $consumerInfo" }
                    FetchPosition(getShardIteratorBySequenceNumber(existingSequenceNumber), existingSequenceNumber)
                } else {
                    // If the import address is defined, we try to import the sequence number to start from DynamoDB (KCL V1)
                    val importedSequenceNbr = if (options.sequenceNbrImportAddress.isNotNull()) {
                        vertx.eventBus().requestAwait<String>(options.sequenceNbrImportAddress, "$shardId").body()
                    } else null

                    if (importedSequenceNbr.isNotNull()) {
                        logger.debug { "Use KCL V1 imported sequence number for $consumerInfo" }
                        FetchPosition(
                            getShardIteratorBySequenceNumber(
                                SequenceNumber(importedSequenceNbr, SequenceNumberIteratorPosition.AFTER)
                            ), existingSequenceNumber
                        )
                    } else {
                        logger.debug { "Use ${ShardIteratorType.LATEST.name} shard iterator for $consumerInfo because no existing position found" }
                        FetchPosition(kinesisClient.getLatestShardIteratorAwait(options.streamName, shardId), null)
                    }
                }
            }
        }
    }

    private suspend fun getShardIteratorBySequenceNumber(existingSequenceNumber: SequenceNumber): ShardIterator {
        return kinesisClient.getShardIteratorBySequenceNumberAwait(
            options.streamName,
            shardId,
            existingSequenceNumber
        )
    }
}
