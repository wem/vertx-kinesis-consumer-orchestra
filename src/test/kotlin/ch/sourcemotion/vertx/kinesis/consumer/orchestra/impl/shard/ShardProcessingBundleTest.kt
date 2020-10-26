package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.createShard
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.services.kinesis.model.Shard

internal class ShardProcessingBundleTest {

    /**
     * 10 fresh shard without inheritance.
     */
    @Test
    internal fun do_all_shards_no_shard_finished() {
        val availableIds = ShardIdGenerator.generateShardIdList(10)
        val processingBundle = ShardProcessingBundle.create(
            availableIds.map { createShard(it) },
            listOf(),
            LoadConfiguration.createDoAllShardsConfig()
        )
        processingBundle.shardIds.shouldContainExactlyInAnyOrder(availableIds)
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with unfinished parents.
     */
    @Test
    internal fun do_all_shards_not_finished_and_merged_with_not_finished_parents() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            emptyList(),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
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
    internal fun do_all_shards_not_finished_and_merged_with_not_finished_parent() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeAdjacentParent.shardId().asShardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(mergeParent.shardIdTyped(), notFinished.shardIdTyped())
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with a finished adjacent parent and unfinished parent.
     */
    @Test
    internal fun do_all_shards_not_finished_and_merged_with_not_finished_adjacent() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            mergeAdjacentParent.shardIdTyped(),
            notFinished.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with finished parents.
     */
    @Test
    internal fun do_all_shards_not_finished_and_merged_with_finished_parents() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, mergeParent, mergeAdjacentParent, mergeChild),
            listOf(mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(notFinished.shardIdTyped(), mergeChild.shardIdTyped())
    }

    /**
     * - 1 finished shard without inheritance.
     * - 1 merge child shard with finished parents.
     */
    @Test
    internal fun do_all_shards_finished_and_merged_with_finished_parents() {
        val finished = createShard(ShardIdGenerator.generateShardId())
        val (mergeParent, mergeAdjacentParent, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(mergeParent, mergeAdjacentParent, mergeChild),
            listOf(finished.shardIdTyped(), mergeParent.shardIdTyped(), mergeAdjacentParent.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(mergeChild.shardIdTyped())
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 1 merge child shard with unavailable parents.
     */
    @Test
    internal fun do_all_shards_not_finished_and_merged_with_unavailable_parents() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (_, _, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, mergeChild),
            listOf(),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(notFinished.shardIdTyped(), mergeChild.shardIdTyped())
    }

    /**
     * - 1 finished shard without inheritance.
     * - 1 merge child shard with unavailable parents.
     */
    @Test
    internal fun do_all_shards_finished_and_merged_with_unavailable_parents() {
        val finished = createShard(ShardIdGenerator.generateShardId())
        val (_, _, mergeChild) = createMergeInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(mergeChild),
            listOf(finished.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(mergeChild.shardIdTyped())
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 2 split children shards with finished parent.
     */
    @Test
    internal fun do_all_shards_not_finished_and_split_with_finished_parent() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = createSplitInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(splitParent.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(), splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 2 split children shards with unfinished parent.
     */
    @Test
    internal fun do_all_shards_not_finished_and_split_with_not_finished_parent() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = createSplitInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, splitParent, splitChildLeft, splitChildRight),
            listOf(),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            splitParent.shardIdTyped(), notFinished.shardIdTyped()
        )
    }

    /**
     * - 1 finished shard without inheritance.
     * - 2 split children shards with finished parent.
     */
    @Test
    internal fun do_all_shards_finished_and_split_with_finished_parent() {
        val finished = createShard(ShardIdGenerator.generateShardId())
        val (splitParent, splitChildLeft, splitChildRight) = createSplitInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(finished, splitParent, splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped(), splitParent.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 unfinished shard without inheritance.
     * - 2 split children shards with no more available parent.
     */
    @Test
    internal fun do_all_shards_not_finished_and_split_with_unavailable_parent() {
        val notFinished = createShard(ShardIdGenerator.generateShardId())
        val (_, splitChildLeft, splitChildRight) = createSplitInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(notFinished, splitChildLeft, splitChildRight),
            listOf(),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            notFinished.shardIdTyped(), splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 finished shard without inheritance.
     * - 2 split children shards with no more available parent.
     */
    @Test
    internal fun do_all_shards_finished_and_split_with_unavailable_parent() {
        val finished = createShard(ShardIdGenerator.generateShardId())
        val (_, splitChildLeft, splitChildRight) = createSplitInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped()),
            LoadConfiguration.createDoAllShardsConfig()
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(
            splitChildLeft.shardIdTyped(), splitChildRight.shardIdTyped()
        )
    }

    /**
     * - 1 finished shard without inheritance.
     * - 2 split children shards with no more available parent.
     */
    @Test
    internal fun exact_finished_and_split_with_unavailable_parent() {
        val finished = createShard(ShardIdGenerator.generateShardId())
        val (_, splitChildLeft, splitChildRight) = createSplitInheritance()

        val processingBundle = ShardProcessingBundle.create(
            listOf(splitChildLeft, splitChildRight),
            listOf(finished.shardIdTyped()),
            LoadConfiguration.createExactConfig(1)
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(splitChildLeft.shardIdTyped())
    }

    /**
     * - 1 finished shard without inheritance.
     * - 1 unfinished shard without inheritance.
     */
    @Test
    internal fun exact_finished_and_not_finished() {
        val finished = createShard(ShardIdGenerator.generateShardId())
        val notFinished = createShard(ShardIdGenerator.generateShardId(1))

        val processingBundle = ShardProcessingBundle.create(
            listOf(finished, notFinished),
            listOf(finished.shardIdTyped()),
            LoadConfiguration.createExactConfig(2)
        )

        processingBundle.shardIds.shouldContainExactlyInAnyOrder(notFinished.shardIdTyped())
    }

    @Test
    internal fun illegal_load_configuration() {
        assertThrows<VertxKinesisConsumerOrchestraException> {
            ShardProcessingBundle.create(
                listOf(),
                listOf(),
                LoadConfiguration(LoadStrategy.EXACT, null)
            )
        }
    }

    private fun createSplitInheritance() : Triple<Shard, Shard, Shard> {
        val splitParent = createShard(ShardIdGenerator.generateShardId(1))
        val splitChildLeft = createShard(ShardIdGenerator.generateShardId(2), splitParent.shardIdTyped())
        val splitChildRight = createShard(ShardIdGenerator.generateShardId(3), splitParent.shardIdTyped())
        return Triple(splitParent, splitChildLeft, splitChildRight)
    }

    private fun createMergeInheritance() : Triple<Shard, Shard, Shard> {
        val mergeParent = createShard(ShardIdGenerator.generateShardId(1))
        val mergeAdjacentParent = createShard(ShardIdGenerator.generateShardId(2))
        val mergeChild = createShard(
            ShardIdGenerator.generateShardId(3),
            mergeParent.shardIdTyped(),
            mergeAdjacentParent.shardIdTyped()
        )
        return Triple(mergeParent, mergeAdjacentParent, mergeChild)
    }
}
