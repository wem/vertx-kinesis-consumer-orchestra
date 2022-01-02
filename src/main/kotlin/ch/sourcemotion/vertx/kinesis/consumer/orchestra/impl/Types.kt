package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.isNotNullOrBlank
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import io.vertx.codegen.annotations.DataObject
import io.vertx.core.json.JsonObject
import software.amazon.awssdk.services.kinesis.model.Shard

internal typealias ShardList = List<Shard>
internal typealias ShardIdList = List<ShardId>

data class OrchestraClusterNodeId(val clusterName: String, val nodeId: String) {

    constructor(name: OrchestraClusterName, id: String) : this("$name", id)

    companion object {
        @JvmStatic
        @JsonCreator
        fun of(value: String) : OrchestraClusterNodeId {
            val parts = value.split("::")
            if (parts.size != 2) {
                throw VertxKinesisConsumerOrchestraException("$value is not a valid Orchestra cluster node id")
            }
            return OrchestraClusterNodeId(parts[0], parts[1])
        }
    }

    @JsonValue
    override fun toString() = "$clusterName::$nodeId"
}

data class OrchestraClusterName(val applicationName: String, val streamName: String) {
    @JsonValue
    override fun toString() = "$applicationName-$streamName"
}

internal data class ShardIterator(val iter: String) {
    companion object {
        fun of(iteratorValue: String?) = if (iteratorValue.isNotNullOrBlank()) {
            ShardIterator(iteratorValue)
        } else null
    }

    override fun toString() = iter
}

internal fun String.asShardIteratorTyped() = ShardIterator(this)

/**
 * [iteratorPosition] determines the position on which shard iterator should be queried from Kinesis for the [number].
 */
data class SequenceNumber(val number: String, val iteratorPosition: SequenceNumberIteratorPosition) {
    companion object {
        fun after(sequenceNumber: String?) = if (sequenceNumber.isNotNullOrBlank()) {
            SequenceNumber(sequenceNumber, SequenceNumberIteratorPosition.AFTER)
        } else null
    }
}

enum class SequenceNumberIteratorPosition { AFTER, AT }

internal fun String.asSequenceNumberAt() = SequenceNumber(this, SequenceNumberIteratorPosition.AT)
internal fun String.asSequenceNumberAfter() = SequenceNumber(this, SequenceNumberIteratorPosition.AFTER)

@DataObject
data class ShardId(val id: String) {

    companion object {
        @JvmStatic
        @JsonCreator
        fun create(value: String) = ShardId(value)
    }

    constructor(json: JsonObject) : this(json.getString("id"))

    fun toJson(): JsonObject = JsonObject().put("id", id)

    @JsonValue
    override fun toString() = id
}

internal fun String.asShardIdTyped(): ShardId = ShardId(this)
