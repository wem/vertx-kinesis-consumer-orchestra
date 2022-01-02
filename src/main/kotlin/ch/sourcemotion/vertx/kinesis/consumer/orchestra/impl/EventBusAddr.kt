package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

/**
 * Central place to hold event bus addresses
 */
internal object EventBusAddr {

    private const val ADDR_BASE = "/vkco"

    val resharding = Resharding

    object Resharding {
        private const val ADDR_BASE = "${EventBusAddr.ADDR_BASE}/resharding"
        const val notification = "$ADDR_BASE/notification/resharding-event"
    }
}
