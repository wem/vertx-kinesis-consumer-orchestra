package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shardOf
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Shard

internal class ConsumerShardIdListFactoryTest {

    private companion object {
        const val MAX_MAX_SHARD_COUNT = Int.MAX_VALUE
    }

    private val sut = ConsumerShardIdListFactory


    /**
     * 10 fresh shard without inheritance.
     */
    @Test
    internal fun no_shard_finished() {
        val availableIds = ShardIdGenerator.generateShardIdList(10)
        sut.create(
            availableIds.map { shardOf(it) },
            listOf(),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(availableIds)
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with unfinished parents.
     */
    @Test
    internal fun not_finished_and_merged_with_not_finished_parents() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            emptyList(),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            mergeParent.shardIdTyped(),
            mergeAdjacentParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with a finished parent and unfinished adjacent parent.
     */
    @Test
    internal fun not_finished_and_merged_with_not_finished_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeAdjacentParent.shardId().asShardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(mergeParent.shardIdTyped(), notFinished.shardIdTyped())
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with a finished adjacent parent and unfinished parent.
     */
    @Test
    internal fun not_finished_and_merged_with_not_finished_adjacent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            mergeAdjacentParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with finished parents.
     */
    @Test
    internal fun not_finished_and_merged_with_finished_parents() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(notFinished.shardIdTyped(), mergeChild.shardIdTyped())
    }

    /**
     * - 1 finished shard without inheritance.
     * - 1 merge child shard with finished parents.
     */
    @Test
    internal fun finished_and_merged_with_finished_parents() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(mergeParent, mergeAdjacentParent, mergeChild),
            listOf(finished.shardIdTyped(), mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(mergeChild.shardIdTyped())
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with unavailable parents.
     */
    @Test
    internal fun not_finished_and_merged_with_unavailable_parents() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (_, _, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(notFinished, mergeChild),
            listOf(),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(notFinished.shardIdTyped(), mergeChild.shardIdTyped())
    }

    /**
     * - 1 finished shard without inheritance.
     * - 1 merge child shard with unavailable parents.
     */
    @Test
    internal fun finished_and_merged_with_unavailable_parents() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (_, _, mergeChild) = createMergeInheritance()

        sut.create(
            listOf(mergeChild),
            listOf(finished.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(mergeChild.shardIdTyped())
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 2 split children shards with finished parent.
     */
    @Test
    internal fun not_finished_and_split_with_finished_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = createSplitInheritance()

        sut.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(splitParent.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(), splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 2 split children shards with unfinished parent.
     */
    @Test
    internal fun not_finished_and_split_with_not_finished_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = createSplitInheritance()

        sut.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            splitParent.shardIdTyped(), notFinished.shardIdTyped()
        )
    }

    /**
     * - 1 finished shard without inheritance.
     * - 2 split children shards with finished parent.
     */
    @Test
    internal fun finished_and_split_with_finished_parent() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = createSplitInheritance()

        sut.create(
            listOf(finished, splitParent, splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped(), splitParent.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 2 split children shards with no more available parent.
     */
    @Test
    internal fun not_finished_and_split_with_unavailable_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (_, splitChildLeft, splitChildRight) = createSplitInheritance()

        sut.create(
            listOf(notFinished, splitChildLeft, splitChildRight),
            listOf(),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(), splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 finished shard without inheritance.
     * - 2 split children shards with no more available parent.
     */
    @Test
    internal fun finished_and_split_with_unavailable_parent() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (_, splitChildLeft, splitChildRight) = createSplitInheritance()

        sut.create(
            listOf(splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped()),
            MAX_MAX_SHARD_COUNT
        ).shouldContainExactlyInAnyOrder(
            splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 finished shard without inheritance.
     * - 2 split children shards with no more available parent.
     */
    @Test
    internal fun exact_finished_and_split_with_unavailable_parent() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (_, splitChildLeft, splitChildRight) = createSplitInheritance()

        sut.create(
            listOf(splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped()),
            1
        ).shouldContainExactlyInAnyOrder(splitChildLeft.shardIdTyped())
    }

    /**
     * - 1 finished shard without inheritance.
     * - 1 unfinished shard without inheritance.
     */
    @Test
    internal fun exact_finished_and_not_finished() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val notFinished = shardOf(ShardIdGenerator.generateShardId(1))

        sut.create(
            listOf(finished, notFinished),
            listOf(finished.shardIdTyped()),
            2
        ).shouldContainExactlyInAnyOrder(notFinished.shardIdTyped())
    }

    private fun createSplitInheritance(): Triple<Shard, Shard, Shard> {
        val splitParent = shardOf(ShardIdGenerator.generateShardId(1))
        val splitChildLeft = shardOf(ShardIdGenerator.generateShardId(2), splitParent.shardIdTyped())
        val splitChildRight = shardOf(ShardIdGenerator.generateShardId(3), splitParent.shardIdTyped())
        return Triple(splitParent, splitChildLeft, splitChildRight)
    }

    private fun createMergeInheritance(): Triple<Shard, Shard, Shard> {
        val mergeParent = shardOf(ShardIdGenerator.generateShardId(1))
        val mergeAdjacentParent = shardOf(ShardIdGenerator.generateShardId(2))
        val mergeChild = shardOf(
            ShardIdGenerator.generateShardId(3),
            mergeParent.shardIdTyped(),
            mergeAdjacentParent.shardIdTyped()
        )
        return Triple(mergeParent, mergeAdjacentParent, mergeChild)
    }
}
