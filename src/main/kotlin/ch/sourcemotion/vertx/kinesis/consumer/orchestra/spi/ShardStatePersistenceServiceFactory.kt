package ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.serviceproxy.ServiceBinder
import io.vertx.serviceproxy.ServiceProxyBuilder

/**
 * Factory to expose the shard state persistence service (verticle).
 */
object ShardStatePersistenceServiceFactory {
    private const val ADDRESS = "/kinesis-consumer-orchester/shard-persistence/service"

    suspend fun expose(vertx: Vertx, serviceInstance: ShardStatePersistenceService): MessageConsumer<JsonObject> {
        return awaitResult<MessageConsumer<JsonObject>> {
            expose(vertx, serviceInstance, it)
        }.also { it.completion().await() }
    }

    fun expose(
        vertx: Vertx,
        serviceInstance: ShardStatePersistenceService,
        handler: Handler<AsyncResult<MessageConsumer<JsonObject>>>
    ) {
        val consumer = ServiceBinder(vertx).setAddress(ADDRESS)
            .registerLocal(ShardStatePersistenceService::class.java, serviceInstance)
        consumer.completionHandler { consumerResult ->
            if (consumerResult.succeeded()) {
                handler.handle(Future.succeededFuture(consumer))
            } else {
                handler.handle(Future.failedFuture(consumerResult.cause()))
            }
        }
    }

    internal fun createAsyncShardStatePersistenceService(vertx: Vertx): ShardStatePersistenceServiceAsync {
        val delegate = ServiceProxyBuilder(vertx).setAddress(ADDRESS)
            .setOptions(DeliveryOptions().setLocalOnly(true)).build(
                ShardStatePersistenceService::class.java
            )
        return ShardStatePersistenceServiceAsync(delegate)
    }
}
