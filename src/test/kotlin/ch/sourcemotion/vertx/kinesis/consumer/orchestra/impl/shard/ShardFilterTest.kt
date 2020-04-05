package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.asShardIdTyped
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotlintest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotlintest.matchers.types.shouldNotBeNull
import io.kotlintest.shouldBe
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Shard

internal class ShardFilterTest {

    @Test
    fun getMergeChildShard() {
        val parentShard = mock<Shard> { on { shardId() } doReturn "shardId-000000000000" }
        val adjacentParent = mock<Shard> { on { shardId() } doReturn "shardId-000000000001" }

        val childShard = mock<Shard> {
            on { shardId() } doReturn "shardId-000000000002"
            on { parentShardId() } doAnswer { parentShard.shardId() }
            on { adjacentParentShardId() } doAnswer { adjacentParent.shardId() }
        }

        ShardFilter.getMergeChildShard(
            listOf(parentShard, adjacentParent, childShard),
            parentShard.shardId().asShardIdTyped()
        ).let {
            it.shouldNotBeNull()
            it.shardId().shouldBe(childShard.shardId())
        }

        ShardFilter.getMergeChildShard(
            listOf(parentShard, adjacentParent, childShard),
            adjacentParent.shardId().asShardIdTyped()
        ).let {
            it.shouldNotBeNull()
            it.shardId().shouldBe(childShard.shardId())
        }
    }

    @Test
    fun getSplitChildrenShards() {
        val parentShard = mock<Shard> { on { shardId() } doReturn "shardId-000000000000" }
        val childShardLeft = mock<Shard> {
            on { shardId() } doReturn "shardId-000000000001"
            on { parentShardId() } doAnswer { parentShard.shardId() }
        }
        val childShardRight = mock<Shard> {
            on { shardId() } doReturn "shardId-000000000002"
            on { parentShardId() } doAnswer { parentShard.shardId() }
        }
        val destinationShardIds = ShardFilter.getSplitChildrenShards(
            listOf(parentShard, childShardLeft, childShardRight),
            parentShard.shardId().asShardIdTyped()
        ).map { it.shardId() }
        destinationShardIds.shouldContainExactlyInAnyOrder(childShardLeft.shardId(), childShardRight.shardId())
    }
}
