package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import software.amazon.awssdk.services.kinesis.model.Shard
import java.net.URI


fun String.toUri(): URI = URI.create(this)

fun createShardMock(
    shardId: String,
    parentShardId: String? = null,
    adjacentParentShardId: String? = null
): Shard = mock {
    on { shardId() } doReturn shardId
    on { parentShardId() } doReturn parentShardId
    on { adjacentParentShardId() } doReturn adjacentParentShardId
    on { toString() } doReturn shardId
}
