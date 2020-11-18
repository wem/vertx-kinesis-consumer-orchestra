package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

/**
 * Central place to hold event bus addresses
 */
class EventBusAddr(clusterName: OrchestraClusterName) {

    private companion object {
        const val ADDR_BASE = "/vkco"
    }

    val resharding = Resharding(clusterName)

    class Resharding(private val clusterName: OrchestraClusterName) {
        private companion object {
            const val ADDR_BASE = "${EventBusAddr.ADDR_BASE}/resharding"
        }

        val reshardingNotification = "$ADDR_BASE/notification/resharding-event"
        val stopConsumerCmd = "$ADDR_BASE/cmd/stop-consumer"
        val startConsumerCmd = "$ADDR_BASE/cmd/start-consumer"
        val readyForStartConsumerCmd = "$ADDR_BASE/cmd/ready-for-start-consumer-cmd"

        val communication = VertxCommunication(clusterName)

        class VertxCommunication(private val clusterName: OrchestraClusterName) {
            private companion object {
                const val ADDR_BASE = "${Resharding.ADDR_BASE}/communication"
            }

            val clusterStartConsumerCmd = "$clusterName/$ADDR_BASE/start-consumer"
        }
    }
}
