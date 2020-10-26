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

data class MergeReshardingEvent(
    val finishedParentShardId: ShardId,
    val childShardId: ShardId
) : ReshardingEvent(ReshardingType.MERGE)

class SplitReshardingEvent(
    val finishedParentShardId: ShardId,
    val childShardIds: ShardIdList
) : ReshardingEvent(ReshardingType.SPLIT)
