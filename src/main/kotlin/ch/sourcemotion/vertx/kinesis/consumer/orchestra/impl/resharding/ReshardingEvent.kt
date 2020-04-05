package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardIdList
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.vertx.core.shareddata.Shareable

enum class ReshardingType {
    SPLIT,
    MERGE
}

@JsonTypeInfo(
    use = JsonTypeInfo.Id.CLASS,
    include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes(
    JsonSubTypes.Type(value = MergeReshardingEvent::class, name = "merge"),
    JsonSubTypes.Type(value = SplitReshardingEvent::class, name = "split")
)
abstract class ReshardingEvent(val reshardingType: ReshardingType) : Shareable {
    companion object {
        const val NOTIFICATION_ADDR = "/kinesis-consumer-orchester/consumer/resharding"
    }

    fun getNotificationAddr() = NOTIFICATION_ADDR
}

class MergeReshardingEvent(
    val parentShardId: ShardId,
    val adjacentParentShardId: ShardId,
    val childShardId: ShardId,
    val finishedShardId: ShardId
) : ReshardingEvent(ReshardingType.MERGE) {

    override fun toString(): String {
        return "MergeReshardingInformation(parentShardId='$parentShardId', adjacentParentShardId='$adjacentParentShardId', childShardId='$childShardId')"
    }
}

class SplitReshardingEvent(val parentShardId: ShardId, val childShardIds: ShardIdList) :
    ReshardingEvent(ReshardingType.SPLIT) {

    override fun toString(): String {
        return "SplitReshardingInformation(parentShard='$parentShardId' resultingShards=$childShardIds)"
    }
}
