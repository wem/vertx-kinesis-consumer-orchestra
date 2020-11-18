package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding.communication

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import io.vertx.junit5.VertxTestContext

internal interface OrchestraCommunicationTestDefinition {
    companion object {
        val shardId = ShardId("afdfd70c-23ce-42cc-9922-553de349b8b7")
        val clusterName = OrchestraClusterName("test-application", "test-stream")
    }

    fun common_workflow(testContext: VertxTestContext)

    fun command_receiver_fail(testContext: VertxTestContext)

    fun command_to_ifself(testContext: VertxTestContext)

    fun multiple_potential_receivers(testContext: VertxTestContext)

    fun no_command_received_if_not_ready(testContext: VertxTestContext)
}
