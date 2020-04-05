package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test


internal class ReshardingTest : AbstractKinesisAndRedisTest() {

    @Test
    internal fun getRecordsOnMerge(vertx: Vertx, testContext: VertxTestContext) {
        testContext.completeNow()
    }
}
