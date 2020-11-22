package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import software.amazon.awssdk.services.kinesis.model.Shard

fun shardOf(shardId: ShardId, parentShardId: ShardId? = null, adjacentParentShardId: ShardId? = null) = mock<Shard> {
    on { shardId() } doReturn "$shardId"
    on { parentShardId() } doReturn parentShardId?.let { "$it" }
    on { adjacentParentShardId() } doReturn adjacentParentShardId?.let { "$it" }
}
