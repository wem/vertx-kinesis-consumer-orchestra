package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

/**
 * Central place to hold event bus addresses
 */
object EventBusAddr {

    private const val ADDR_BASE = "/vkco"

    val resharding = Resharding
    val consumerControl = ConsumerControl
    val detection = Detection

    object Resharding {
        private const val ADDR_BASE = "${EventBusAddr.ADDR_BASE}/resharding"
        const val notification = "$ADDR_BASE/notification/resharding-event"
    }

    object ConsumerControl {
        private const val ADDR_BASE = "${EventBusAddr.ADDR_BASE}/consumer-orchestration"
        const val stopConsumerCmd = "$ADDR_BASE/cmd/stop-consumer"
        const val startConsumersCmd = "$ADDR_BASE/cmd/start-consumers"
    }

    object Detection {
        private const val ADDR_BASE = "${EventBusAddr.ADDR_BASE}/detection"
        const val shardsConsumedCountNotification = "$ADDR_BASE/notification/shards-consumed"
    }
}
