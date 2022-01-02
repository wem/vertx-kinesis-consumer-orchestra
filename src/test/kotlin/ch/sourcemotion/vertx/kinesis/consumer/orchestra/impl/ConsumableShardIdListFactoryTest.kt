package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.shardOf
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Shard

internal class ConsumableShardIdListFactoryTest {

    private val sut = ConsumableShardIdListFactory

    @Test
    internal fun no_shard_finished() {
        val allAndAvailableShards = ShardIdGenerator.generateShardIdList(10)
        sut.create(
            allAndAvailableShards.map { shardOf(it) },
            allAndAvailableShards.map { shardOf(it) },
            emptyList()
        ).shouldContainExactly(allAndAvailableShards)
    }

    @Test
    internal fun exact_no_shard_finished() {
        val allAndAvailableShards = ShardIdGenerator.generateShardIdList(10)
        sut.create(
            allAndAvailableShards.map { shardOf(it) },
            allAndAvailableShards.map { shardOf(it) },
            emptyList()
        ).shouldContainExactly(allAndAvailableShards)
    }

    @Test
    internal fun one_shard_finished() {
        val allShardIds = ShardIdGenerator.generateShardIdList(10)
        val finishedShardId = allShardIds.last()
        val availableShardIds = allShardIds.filterNot { finishedShardId == it }
        sut.create(
            allShardIds.map { shardOf(it) },
            availableShardIds.map { shardOf(it) },
            listOf(finishedShardId)
        ).shouldContainExactly(availableShardIds)
    }

    @Test
    internal fun not_finished_and_merge_parents_not_finished() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            emptyList()
        ).shouldContainExactlyInAnyOrder(
            mergeParent.shardIdTyped(),
            mergeAdjacentParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    @Test
    internal fun not_finished_and_merged_with_finished_adjacentParent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(notFinished, mergeParent, mergeChild),
            listOf(mergeAdjacentParent.shardIdTyped())
        ).shouldContainExactlyInAnyOrder(mergeParent.shardIdTyped(), notFinished.shardIdTyped())
    }

    @Test
    internal fun not_finished_and_merged_with_finished_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(notFinished, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped())
        ).shouldContainExactlyInAnyOrder(mergeAdjacentParent.shardIdTyped(), notFinished.shardIdTyped())
    }

    @Test
    internal fun not_finished_and_merged_with_finished_parents() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(notFinished, mergeChild, mergeAdjacentParent, mergeChild),
            listOf(notFinished, mergeChild),
            listOf(mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped())
        ).shouldContainExactlyInAnyOrder(notFinished.shardIdTyped(), mergeChild.shardIdTyped())
    }

    @Test
    internal fun finished_and_merged_with_finished_parents() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(finished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeChild),
            listOf(finished.shardIdTyped(), mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped())
        ).shouldContainExactly(mergeChild.shardIdTyped())
    }

    @Test
    internal fun not_finished_and_merged_with_unavailable_parents() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(notFinished, mergeChild),
            emptyList()
        ).shouldContainExactly(notFinished.shardIdTyped())
    }

    @Test
    internal fun finished_and_merged_with_unavailable_parents() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(finished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeChild),
            listOf(finished.shardIdTyped())
        ).shouldBeEmpty()
    }

    @Test
    internal fun merged_with_unavailable_parent_and_finished_adjacent() {
        val (mergeParent, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeChild),
            listOf(mergeAdjacentParent.shardIdTyped())
        ).shouldBeEmpty()
    }

    @Test
    internal fun merged_with_not_existing_parents() {
        val (_, _, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeChild),
            listOf(mergeChild),
            emptyList()
        ).shouldContainExactly(mergeChild.shardIdTyped())
    }

    @Test
    internal fun merged_with_not_existing_and_not_finished_parent() {
        val (_, _, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeChild),
            listOf(mergeChild),
            emptyList()
        ).shouldContainExactly(mergeChild.shardIdTyped())
    }

    @Test
    internal fun merged_with_not_existing_parent_and_finished_adjacent() {
        val (_, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeAdjacentParent, mergeChild),
            listOf(mergeChild),
            listOf(mergeAdjacentParent.shardIdTyped())
        ).shouldContainExactly(mergeChild.shardIdTyped())
    }

    @Test
    internal fun merged_with_not_existing_and_unavailable_parent() {
        val (_, _, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeChild),
            listOf(mergeChild),
            emptyList(),
        ).shouldContainExactly(mergeChild.shardIdTyped())
    }

    @Test
    internal fun merged_with_not_existing_parent_unavailable_adjacent() {
        val (_, mergeAdjacentParent, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeAdjacentParent, mergeChild),
            listOf(mergeChild),
            emptyList(),
        ).shouldBeEmpty()
    }

    @Test
    internal fun merged_with_not_existing_adjacent_unavailable_parent() {
        val (mergeParent, _, mergeChild) = mergeShardInheritance()

        sut.create(
            listOf(mergeParent, mergeChild),
            listOf(mergeChild),
            emptyList(),
        ).shouldBeEmpty()
    }

    @Test
    internal fun not_finished_and_split_with_finished_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(notFinished, splitChildLeft, splitChildRight),
            listOf(splitParent.shardIdTyped()),
        ).shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(), splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    @Test
    internal fun not_finished_and_split_with_not_finished_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            emptyList(),
        ).shouldContainExactlyInAnyOrder(splitParent.shardIdTyped(), notFinished.shardIdTyped())
    }

    @Test
    internal fun finished_and_split_with_finished_parent() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(finished, splitParent, splitChildLeft, splitChildRight),
            listOf(splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped(), splitParent.shardIdTyped()),
        ).shouldContainExactlyInAnyOrder(splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped())
    }

    @Test
    internal fun not_finished_and_split_with_unavailable_parent() {
        val notFinished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(notFinished, splitChildLeft, splitChildRight),
            emptyList(),
        ).shouldContainExactlyInAnyOrder(notFinished.shardIdTyped())
    }

    @Test
    internal fun finished_and_split_with_unavailable_parent() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(finished, splitParent, splitChildLeft, splitChildRight),
            listOf(splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped()),
        ).shouldBeEmpty()
    }

    @Test
    internal fun split_with_unavailable_parent() {
        val (splitParent, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(splitParent, splitChildLeft, splitChildRight),
            listOf(splitChildLeft, splitChildRight),
            emptyList(),
        ).shouldBeEmpty()
    }

    @Test
    internal fun split_with_not_existing_parent() {
        val (_, splitChildLeft, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(splitChildLeft, splitChildRight),
            listOf(splitChildLeft, splitChildRight),
            emptyList(),
        ).shouldContainExactlyInAnyOrder(splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped())
    }

    @Test
    internal fun split_with_not_existing_parent_and_child() {
        val (_, _, splitChildRight) = splitShardInheritance()

        sut.create(
            listOf(splitChildRight),
            listOf(splitChildRight),
            emptyList(),
        ).shouldContainExactly(splitChildRight.shardIdTyped())
    }

    @Test
    internal fun finished_and_not_finished() {
        val finished = shardOf(ShardIdGenerator.generateShardId())
        val notFinished = shardOf(ShardIdGenerator.generateShardId(1))

        sut.create(
            listOf(finished, notFinished),
            listOf(notFinished),
            listOf(finished.shardIdTyped()),
        ).shouldContainExactly(notFinished.shardIdTyped())
    }

    private fun splitShardInheritance(): Triple<Shard, Shard, Shard> {
        val splitParent = shardOf(ShardIdGenerator.generateShardId(1))
        val splitChildLeft = shardOf(ShardIdGenerator.generateShardId(2), splitParent.shardIdTyped())
        val splitChildRight = shardOf(ShardIdGenerator.generateShardId(3), splitParent.shardIdTyped())
        return Triple(splitParent, splitChildLeft, splitChildRight)
    }

    private fun mergeShardInheritance(): Triple<Shard, Shard, Shard> {
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
