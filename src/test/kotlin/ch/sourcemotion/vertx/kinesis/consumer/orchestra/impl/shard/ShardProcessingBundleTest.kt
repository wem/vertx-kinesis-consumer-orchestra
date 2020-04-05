package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.createShardMock
import io.kotlintest.matchers.collections.shouldContainExactly
import io.kotlintest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test

internal class ShardProcessingBundleTest {

    @Test
    internal fun all_shards_not_finished() {
        val shardIds = ShardIdGenerator.generateShardIdList(10)
        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            shardIds.map { createShardMock(it.id) }, listOf()
        )
        processingBundle.shardIds.shouldContainExactlyInAnyOrder(shardIds)
    }

    @Test
    internal fun not_finished_and_merged_with_not_finished_parents() {
        val notFinished =
            createShardMock(ShardIdGenerator.generateShardId().id)

        val mergeParent =
            createShardMock(ShardIdGenerator.generateShardId(1).id)
        val mergeAdjacentParent =
            createShardMock(ShardIdGenerator.generateShardId(2).id)
        val mergeChild = createShardMock(
            ShardIdGenerator.generateShardId(3).id,
            mergeParent.shardId(),
            mergeAdjacentParent.shardId()
        )

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            emptyList()
        )

        processingBundle.shardIds.shouldContainExactly(
            mergeParent.shardIdTyped(),
            mergeAdjacentParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    @Test
    internal fun not_finished_and_merged_with_not_finished_parent() {
        val notFinished = createShardMock(ShardIdGenerator.generateShardId().id)

        val mergeParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val mergeAdjacentParent = createShardMock(ShardIdGenerator.generateShardId(2).id)
        val mergeChild = createShardMock(
            ShardIdGenerator.generateShardId(3).id,
            mergeParent.shardId(),
            mergeAdjacentParent.shardId()
        )

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeAdjacentParent.shardId().asShardIdTyped())
        )

        processingBundle.shardIds.shouldContainExactly(
            mergeParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    @Test
    internal fun not_finished_and_merged_with_not_finished_adjacent() {
        val notFinished = createShardMock(ShardIdGenerator.generateShardId().id)

        val mergeParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val mergeAdjacentParent = createShardMock(ShardIdGenerator.generateShardId(2).id)
        val mergeChild = createShardMock(
            ShardIdGenerator.generateShardId(3).id,
            mergeParent.shardId(),
            mergeAdjacentParent.shardId()
        )

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped())
        )

        processingBundle.shardIds.shouldContainExactly(
            mergeAdjacentParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    @Test
    internal fun not_finished_and_merged_with_finished_parents() {
        val notFinished = createShardMock(ShardIdGenerator.generateShardId().id)

        val mergeParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val mergeAdjacentParent = createShardMock(ShardIdGenerator.generateShardId(2).id)
        val mergeChild = createShardMock(
            ShardIdGenerator.generateShardId(3).id,
            mergeParent.shardId(),
            mergeAdjacentParent.shardId()
        )

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped())
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(),
            mergeChild.shardIdTyped()
        )
    }

    @Test
    internal fun finished_and_merged_with_finished_parents() {
        val finished = createShardMock(ShardIdGenerator.generateShardId().id)

        val mergeParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val mergeAdjacentParent = createShardMock(ShardIdGenerator.generateShardId(2).id)
        val mergeChild = createShardMock(
            ShardIdGenerator.generateShardId(3).id,
            mergeParent.shardId(),
            mergeAdjacentParent.shardId()
        )

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(finished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(finished.shardIdTyped(), mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped())
        )

        processingBundle.shardIds.shouldContainExactly(mergeChild.shardIdTyped())
    }

    @Test
    internal fun not_finished_and_split_with_finished_parent() {
        val notFinished = createShardMock(ShardIdGenerator.generateShardId().id)
        val splitParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val splitChildLeft = createShardMock(ShardIdGenerator.generateShardId(2).id, splitParent.shardId())
        val splitChildRight = createShardMock(ShardIdGenerator.generateShardId(3).id, splitParent.shardId())

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(splitParent.shardIdTyped())
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(), splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    @Test
    internal fun not_finished_and_split_with_not_finished_parent() {
        val notFinished = createShardMock(ShardIdGenerator.generateShardId().id)
        val splitParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val splitChildLeft = createShardMock(ShardIdGenerator.generateShardId(2).id, splitParent.shardId())
        val splitChildRight = createShardMock(ShardIdGenerator.generateShardId(3).id, splitParent.shardId())

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight), listOf()
        )

        processingBundle.shardIds.shouldContainExactly(
            splitParent.shardIdTyped(), notFinished.shardIdTyped()
        )
    }

    @Test
    internal fun finished_and_split_with_finished_parent() {
        val finished = createShardMock(ShardIdGenerator.generateShardId().id)
        val splitParent = createShardMock(ShardIdGenerator.generateShardId(1).id)
        val splitChildLeft = createShardMock(ShardIdGenerator.generateShardId(2).id, splitParent.shardId())
        val splitChildRight = createShardMock(ShardIdGenerator.generateShardId(3).id, splitParent.shardId())

        val processingBundle = ShardProcessingBundle.createShardProcessingBundle(
            listOf(finished, splitParent, splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped(), splitParent.shardIdTyped())
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }
}
