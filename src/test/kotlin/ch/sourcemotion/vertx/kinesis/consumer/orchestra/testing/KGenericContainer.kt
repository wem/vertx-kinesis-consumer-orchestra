package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network

class KGenericContainer(dockerImageName: String) : GenericContainer<KGenericContainer>(dockerImageName) {
    companion object {
        private val REDIS_IMAGE = "redis:${System.getenv("REDIS_VERSION")}"
        const val REDIS_PORT = 6379
        fun createRedisContainer(network: Network): KGenericContainer =
            KGenericContainer(REDIS_IMAGE).withExposedPorts(REDIS_PORT).withNetwork(network)
    }
}
