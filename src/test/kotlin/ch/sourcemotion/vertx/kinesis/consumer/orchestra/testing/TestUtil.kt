package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import software.amazon.awssdk.services.kinesis.model.Shard


fun createShardMock(
    shardId: ShardId,
    parentShardId: String? = null,
    adjacentParentShardId: String? = null
): Shard = mock {
    on { shardId() } doReturn shardId.id
    on { parentShardId() } doReturn parentShardId
    on { adjacentParentShardId() } doReturn adjacentParentShardId
    on { toString() } doReturn shardId.id
}
