package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.LoadConfiguration
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.createShardMock
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test


internal class OrchestraShardCompositionTest : AbstractVertxTest() {

    @Test
    internal fun processing_bundle_do_all_shards(testContext: VertxTestContext) = asyncTest(testContext) {
        val shards = createShardMocks(4)
        val processingBundle = OrchestraShardComposition(
            shards,
            emptyList(),
            emptyList(),
            LoadConfiguration.createDoAllShardsConfig()
        ).createShardProcessingBundle()

        testContext.verify { processingBundle.shardIds.shouldContainExactlyInAnyOrder(shards.map { it.shardIdTyped() }) }
    }

    @Test
    internal fun processing_bundle_do_all_shards_some_shard_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shards = createShardMocks(4)
            val processingBundle = OrchestraShardComposition(
                shards,
                shards.take(1).map { it.shardIdTyped() },
                emptyList(),
                LoadConfiguration.createDoAllShardsConfig()
            ).createShardProcessingBundle()

            testContext.verify {
                processingBundle.shardIds.shouldContainExactlyInAnyOrder(
                    shards.takeLast(3).map { it.shardIdTyped() })
            }
        }

    @Test
    internal fun processing_bundle_do_all_shards_some_shards_in_progress_and_finished(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shards = createShardMocks(4)
            val processingBundle = OrchestraShardComposition(
                shards,
                shards.take(1).map { it.shardIdTyped() },
                shards.subList(1, 2).map { it.shardIdTyped() },
                LoadConfiguration.createDoAllShardsConfig()
            ).createShardProcessingBundle()

            testContext.verify {
                processingBundle.shardIds.shouldContainExactlyInAnyOrder(
                    shards.takeLast(2).map { it.shardIdTyped() })
            }
        }

    @Test
    internal fun processing_bundle_do_all_shards_all_shards_in_progress_and_finished(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shards = createShardMocks(4)
            val processingBundle = OrchestraShardComposition(
                shards,
                shards.take(2).map { it.shardIdTyped() },
                shards.takeLast(2).map { it.shardIdTyped() },
                LoadConfiguration.createDoAllShardsConfig()
            ).createShardProcessingBundle()

            testContext.verify { processingBundle.shardIds.shouldBeEmpty() }
        }

    @Test
    internal fun processing_bundle_exactly(testContext: VertxTestContext) = asyncTest(testContext) {
        val shards = createShardMocks(4)
        val processingBundle = OrchestraShardComposition(
            shards,
            emptyList(),
            emptyList(),
            LoadConfiguration.createExactConfig(1)
        ).createShardProcessingBundle()

        testContext.verify { processingBundle.shardIds.shouldContainExactly(shards.first().shardIdTyped()) }
    }

    @Test
    internal fun processing_bundle_exactly_some_shard_in_progress(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shards = createShardMocks(4)
            val processingBundle = OrchestraShardComposition(
                shards,
                shards.take(1).map { it.shardIdTyped() },
                emptyList(),
                LoadConfiguration.createExactConfig(1)
            ).createShardProcessingBundle()

            testContext.verify { processingBundle.shardIds.shouldContainExactly(shards[1].shardIdTyped()) }
        }

    @Test
    internal fun processing_bundle_exactly_some_shards_in_progress_and_finished(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shards = createShardMocks(4)
            val processingBundle = OrchestraShardComposition(
                shards,
                shards.take(1).map { it.shardIdTyped() },
                shards.subList(1, 2).map { it.shardIdTyped() },
                LoadConfiguration.createExactConfig(1)
            ).createShardProcessingBundle()

            testContext.verify { processingBundle.shardIds.shouldContainExactly(shards[2].shardIdTyped()) }
        }

    @Test
    internal fun processing_bundle_exactly_all_shards_in_progress_and_finished(testContext: VertxTestContext) =
        asyncTest(testContext) {
            val shards = createShardMocks(4)
            val processingBundle = OrchestraShardComposition(
                shards,
                shards.take(2).map { it.shardIdTyped() },
                shards.takeLast(2).map { it.shardIdTyped() },
                LoadConfiguration.createExactConfig(1)
            ).createShardProcessingBundle()

            testContext.verify { processingBundle.shardIds.shouldBeEmpty() }
        }

    private fun createShardMocks(shardCount: Int): ShardList = ShardIdGenerator.generateShardIdList(
        shardCount
    ).map { createShardMock(it.id) }
}
