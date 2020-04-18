package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.VertxKinesisOrchestraImpl
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.registerKinesisOrchestraModules
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

/**
 * Entry point to create and orchestra instance. Usually there should be only one instance per Vert.x instance.
 *
 * Scaling should be done by defining of many shards per orchestra instance should get proceeded. Or even more efficient,
 * to distribute received Kinesis records over the event bus.
 * To accomplish this, a simple way could be to fair split the bunch of records received in your implementation of
 * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerVerticle] or
 * [ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle] and send those
 * record splits for finally processing over the event bus.
 */
interface VertxKinesisOrchestra {
    companion object {
        /**
         * Creates and directly starts an orchestra instance. "starts" means consumers gets deployed according to the provided configuration.
         */
        @JvmStatic
        fun create(
            vertx: Vertx,
            options: VertxKinesisOrchestraOptions,
            handler: Handler<AsyncResult<VertxKinesisOrchestra>>
        ) {
            CoroutineScope(vertx.dispatcher()).launch {
                createAwait(
                    vertx,
                    options
                )
            }.invokeOnCompletion {
                it?.let { handler.handle(Future.failedFuture(it)) } ?: handler.handle(Future.succeededFuture())
            }
        }

        /**
         * Suspendable variant of [create]
         */
        suspend fun createAwait(vertx: Vertx, options: VertxKinesisOrchestraOptions): VertxKinesisOrchestra {
            configureJacksonKotlin()
            return VertxKinesisOrchestraImpl(vertx, options).apply { startOrchestration() }
        }

        private fun configureJacksonKotlin() {
            DatabindCodec.mapper().registerKinesisOrchestraModules()
            DatabindCodec.prettyMapper().registerKinesisOrchestraModules()
        }
    }
}
