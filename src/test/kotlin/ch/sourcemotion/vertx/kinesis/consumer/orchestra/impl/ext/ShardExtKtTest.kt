package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.createShard
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class ShardExtKtTest {
    @Test
    internal fun parentShardIds_not_a_child_shard() {
        val splitChild = createShard(ShardIdGenerator.generateShardId(0))
        splitChild.parentShardIds().shouldBeEmpty()

        splitChild.isSplitChild().shouldBeFalse()
        splitChild.isMergedChild().shouldBeFalse()

        splitChild.shardIdTyped().shouldBe(splitChild.shardIdTyped())
        splitChild.parentShardIdTyped().shouldBeNull()
        splitChild.adjacentParentShardIdTyped().shouldBeNull()
    }

    @Test
    internal fun parentShardIds_split_shard() {
        val parentShardId = ShardIdGenerator.generateShardId(0)
        val splitChildShardId = ShardIdGenerator.generateShardId(1)
        val splitChild = createShard(splitChildShardId, parentShardId = parentShardId)
        splitChild.parentShardIds().shouldContainExactly(parentShardId)

        splitChild.isSplitChild().shouldBeTrue()
        splitChild.isMergedChild().shouldBeFalse()

        splitChild.shardIdTyped().shouldBe(splitChild.shardIdTyped())
        splitChild.parentShardIdTyped().shouldBe(parentShardId)
        splitChild.adjacentParentShardIdTyped().shouldBeNull()
    }

    @Test
    internal fun parentShardIds_merged_shard() {
        val parentShardId = ShardIdGenerator.generateShardId(1)
        val adjacentParentShardId = ShardIdGenerator.generateShardId(2)
        val mergeChildShardId = ShardIdGenerator.generateShardId(3)
        val mergeChildShard = createShard(
            mergeChildShardId,
            parentShardId = parentShardId,
            adjacentParentShardId = adjacentParentShardId
        )
        mergeChildShard.parentShardIds().shouldContainExactly(parentShardId, adjacentParentShardId)

        mergeChildShard.isSplitChild().shouldBeFalse()
        mergeChildShard.isMergedChild().shouldBeTrue()

        mergeChildShard.shardIdTyped().shouldBe(mergeChildShard.shardIdTyped())
        mergeChildShard.parentShardIdTyped().shouldBe(parentShardId)
        mergeChildShard.adjacentParentShardIdTyped().shouldBe(adjacentParentShardId)
    }
}
