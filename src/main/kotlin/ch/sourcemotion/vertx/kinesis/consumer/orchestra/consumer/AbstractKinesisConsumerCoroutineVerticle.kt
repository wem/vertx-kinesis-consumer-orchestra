package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import kotlinx.coroutines.launch
import software.amazon.awssdk.services.kinesis.model.Record

/**
 * Kotlin variant of the consumer verticle. [onRecords] function called within verticles context coroutine.
 *
 * Is configurable in orchestra options [ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions#consumerVerticleClass]
 */
abstract class AbstractKinesisConsumerCoroutineVerticle : AbstractKinesisConsumerVerticle() {

    override fun onRecords(records: List<Record>, handler: Handler<AsyncResult<Void>>) {
        launch {
            runCatching { onRecordsAsync(records) }.onSuccess { handler.handle(Future.succeededFuture()) }
                .onFailure { handler.handle(Future.failedFuture(it)) }
        }
    }

    protected abstract suspend fun onRecordsAsync(records: List<Record>)
}
