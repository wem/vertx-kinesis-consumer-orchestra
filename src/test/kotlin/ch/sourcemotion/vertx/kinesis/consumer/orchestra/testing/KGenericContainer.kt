package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import org.testcontainers.containers.GenericContainer

class KGenericContainer(dockerImageName: String) : GenericContainer<KGenericContainer>(dockerImageName) {
    companion object {
        const val REDIS_IMAGE = "redis:5.0.6"
        const val REDIS_PORT = 6379
        fun createRedisContainer(): KGenericContainer =
            KGenericContainer(
                REDIS_IMAGE
            )
                .withExposedPorts(REDIS_PORT)
    }
}
