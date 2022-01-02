package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.MergeReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.SplitReshardingEvent
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StartConsumerCmd
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.StopConsumerCmd
import io.vertx.core.eventbus.EventBus

internal object OrchestraCodecs {

    /**
     * Deploys event bus message codecs used by VKCO.
     */
    fun deployCodecs(eventBus: EventBus) {
        deployCodec(eventBus, MergeReshardingEvent::class.java, LocalCodec("merge-resharding-event-codec"))
        deployCodec(eventBus, SplitReshardingEvent::class.java, LocalCodec("split-resharding-event-codec"))
        deployCodec(eventBus, StartConsumerCmd::class.java, LocalCodec("start-consumer-cmd-codec"))
        deployCodec(eventBus, StopConsumerCmd::class.java, LocalCodec("stop-consumer-cmd-codec"))
    }

    private fun <T :  Any> deployCodec(
        eventBus: EventBus,
        dtoClass: Class<T>,
        codec : LocalCodec<T>
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
