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

interface VertxKinesisOrchestra {
    companion object {
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
