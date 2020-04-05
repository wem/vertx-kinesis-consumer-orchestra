package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException
import io.vertx.core.Vertx
import io.vertx.core.shareddata.Shareable

/**
 * Access point to shared instances of the orchestra within a single vertx instance.
 */
internal object SharedData {
    private const val LOCAL_SHARED_MAP_NAME = "local-kinesis-consumer-orchestra-shared-map"
    fun <T : Shareable> shareInstance(vertx: Vertx, instance: T, reference: String) {
        getLocalSharedMap<T>(vertx)[reference] = instance
    }

    inline fun <reified T : Shareable> getSharedInstance(vertx: Vertx, reference: String) =
        getLocalSharedMap<T>(vertx)[reference]
            ?: throw VertxKinesisConsumerOrchestraException(
                "No shared instance of ${T::class.java.name} under reference: \"$reference\" found"
            )

    private fun <T : Shareable> getLocalSharedMap(vertx: Vertx) =
        vertx.sharedData().getLocalMap<String, T>(LOCAL_SHARED_MAP_NAME)
}
