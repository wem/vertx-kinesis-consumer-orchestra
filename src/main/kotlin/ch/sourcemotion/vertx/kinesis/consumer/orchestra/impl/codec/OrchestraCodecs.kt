package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.MergeReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.SplitReshardingEvent
import io.vertx.core.eventbus.EventBus

object OrchestraCodecs {

    /**
     * Deploys event bus message codecs used by orchestra.
     */
    fun deployCodecs(eventBus: EventBus) {
        deployCodec(eventBus, MergeReshardingEvent::class.java, JacksonMessageCodec.create("merge-resharding-event-codec"))
        deployCodec(eventBus, SplitReshardingEvent::class.java, JacksonMessageCodec.create("split-resharding-event-codec"))
        deployCodec(eventBus, ShardId::class.java, JacksonMessageCodec.create("shard-id-codec"))
    }

    private fun <T :  Any> deployCodec(
        eventBus: EventBus,
        dtoClass: Class<T>,
        codec : JacksonMessageCodec<T>
    ) {
        eventBus.runCatching {
            registerDefaultCodec(dtoClass,codec)
        }.onFailure {
            // We catch exception when codecs are registered multiple times.
            if (it !is IllegalStateException) {
                throw it
            }
        }
    }
}
