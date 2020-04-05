package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.MergeReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.SplitReshardingEvent
import io.vertx.core.eventbus.EventBus

object OrchestraCodecs {

    /**
     * Deploys event bus message codecs used by orchestra.
     */
    fun deployCodecs(eventBus: EventBus) {
        eventBus.registerDefaultCodec(
            MergeReshardingEvent::class.java,
            JacksonMessageCodec.create("merge-resharding-event-codec")
        )
        eventBus.registerDefaultCodec(
            SplitReshardingEvent::class.java,
            JacksonMessageCodec.create("split-resharding-event-codec")
        )
    }
}
