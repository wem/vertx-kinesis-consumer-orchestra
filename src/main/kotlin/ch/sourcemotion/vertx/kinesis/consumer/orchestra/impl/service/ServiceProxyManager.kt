package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.service

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.serviceproxy.ServiceBinder
import io.vertx.serviceproxy.ServiceProxyBuilder

/**
 * Manager for [Vert.x service proxies](https://vertx.io/docs/vertx-service-proxy/java/)
 */
abstract class ServiceProxyManager<T: Any>(private val address: String, private val serviceClass: Class<T>) {
    fun exposeService(vertx: Vertx, impl: T) : Future<Void> {
        return ServiceBinder(vertx).setAddress(address)
            .registerLocal(serviceClass, impl).completion()
    }

    fun createService(vertx: Vertx) : T {
        return ServiceProxyBuilder(vertx).setAddress(address)
            .setOptions(deliveryOptionsOf(localOnly = true)).build(serviceClass)
    }
}