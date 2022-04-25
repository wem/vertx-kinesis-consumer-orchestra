package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing.takeAndRemove
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.completion
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.NodeScoreService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.*
import io.kotest.matchers.collections.shouldHaveSize
import io.vertx.core.CompositeFuture
import io.vertx.core.Vertx
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import mu.KLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.awssdk.services.kinesis.model.Shard
import software.amazon.awssdk.services.kinesis.model.StreamDescription
import java.util.concurrent.TimeUnit

internal abstract class AbstractMultiNodeComponentTest : AbstractKinesisAndRedisTest(false) {

    companion object : KLogging() {
        const val RECORD_FAN_OUT_ADDR = "/kinesis/consumer/orchestra/fan-out"
        const val RECORD_COUNT = 100
    }

    private val nodeVertxInstances = ArrayList<Vertx>()

    @AfterEach
    fun closeOrchestra() = asyncBeforeOrAfter {
        CompositeFuture.all(nodeVertxInstances.map { it.close() }).await()
    }

    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun consume_some_records(testContext: VertxTestContext) {
        val nodeCount = 8
        val nodeScore = 2
        val shardCount = nodeCount * nodeScore
        testContext.async(shardCount * RECORD_COUNT) { checkpoint ->
            val shards = createStreamAndDeployNodes(nodeCount, shardCount).shards()

            nodeVertxInstances.forEach { vertx ->
                vertx.eventBus().consumer<Int>(RECORD_FAN_OUT_ADDR) { msg ->
                    val recordCount = msg.body()
                    repeat(recordCount) { checkpoint.flag() }
                }.completion().await()
            }

            val nodeScoreService = nodeScoreService()
            nodeScoreService.awaitScore(shardCount, nodeScore)

            kinesisClient.putRecordsExplicitHashKey(shardCount batchesOf RECORD_COUNT, predefinedShards = shards)
        }
    }

    @Timeout(value = 4, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun split_resharding(testContext: VertxTestContext) {
        val nodeCount = 8
        val initialNodeScore = 2
        val initialShardCount = nodeCount * initialNodeScore
        val splitShardCount = initialShardCount * 2
        val splitNodeScore = initialNodeScore * 2

        testContext.async(RECORD_COUNT * (initialShardCount + splitShardCount)) { checkpoint ->
            // Create stream with initial shard count and deploy nodes
            val streamDescriptionBeforeSplit = createStreamAndDeployNodes(nodeCount, initialShardCount)

            // Register fan out consumers on each node
            nodeVertxInstances.forEach { vertx ->
                vertx.eventBus().consumer<Int>(RECORD_FAN_OUT_ADDR) { msg ->
                    val recordCount = msg.body()
                    repeat(recordCount) { checkpoint.flag() }
                }.completion().await()
            }

            // Create node score service and await any node has consumer expected consumer count started
            val nodeScoreService = nodeScoreService()
            nodeScoreService.awaitScore(initialShardCount, initialNodeScore)

            // Send records even to each shard
            val parentShards = streamDescriptionBeforeSplit.shards()
            kinesisClient.putRecordsExplicitHashKey(
                initialShardCount batchesOf RECORD_COUNT,
                predefinedShards = parentShards
            )

            // Do split resharding
            val childShards = parentShards.map { parentShard ->
                kinesisClient.splitShardFair(parentShard)
                val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
                streamDescriptionAfterSplit.shards().filter { it.parentShardId() == parentShard.shardId() }.also {
                    logger.info { "Split of parentShard ${parentShard.shardId()} done" }
                }
            }.flatten()

            childShards.shouldHaveSize(splitShardCount)

            // Await any node has consumer expected consumer count started and send records even to each shard
            nodeScoreService.awaitScore(splitShardCount, splitNodeScore)
            kinesisClient.putRecordsExplicitHashKey(
                splitShardCount batchesOf RECORD_COUNT, predefinedShards = childShards
            )
        }
    }

    @Timeout(value = 6, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun merge_resharding(testContext: VertxTestContext) {
        val nodeCount = 8
        val initialNodeScore = 4
        val initialShardCount = nodeCount * initialNodeScore
        val mergedShardCount = initialShardCount / 2
        val mergedNodeScore = initialNodeScore / 2
        testContext.async(RECORD_COUNT * (initialShardCount + mergedShardCount)) { checkpoint ->
            // Create stream with initial shard count and deploy nodes
            val streamDescriptionBeforeMerge = createStreamAndDeployNodes(nodeCount, initialShardCount)

            // Register fan out consumers on each node
            nodeVertxInstances.forEach { vertx ->
                vertx.eventBus().consumer<Int>(RECORD_FAN_OUT_ADDR) { msg ->
                    val recordCount = msg.body()
                    repeat(recordCount) { checkpoint.flag() }
                }.completion().await()
            }

            // Create node score service and await any node has consumer expected consumer count started
            val nodeScoreService = nodeScoreService()
            nodeScoreService.awaitScore(initialShardCount, initialNodeScore)

            // Send records even to each shard
            val parentShards = streamDescriptionBeforeMerge.shards()
            kinesisClient.putRecordsExplicitHashKey(
                initialShardCount batchesOf RECORD_COUNT, predefinedShards = parentShards
            )

            // Do merge resharding
            val shards = parentShards.iterator()
            val childShards = ArrayList<Shard>()
            while (shards.hasNext()) {
                val parent = shards.next()
                val adjacentParent = shards.next()
                kinesisClient.mergeShards(parent, adjacentParent)
                val streamDescriptionAfterSplit = kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME)
                val childShard = streamDescriptionAfterSplit.shards()
                    .first { it.parentShardId() == parent.shardId() && it.adjacentParentShardId() == adjacentParent.shardId() }
                childShards.add(childShard)

                logger.info { "Merge of shards ${parent.shardId()} / ${adjacentParent.shardId()} done" }
            }

            childShards.shouldHaveSize(mergedShardCount)
            
            // Await any node has consumer expected consumer count started and send records even to each shard
            nodeScoreService.awaitScore(mergedShardCount, mergedNodeScore)
            delay(30000) // Because of some reason, the hash key range could change for some time after merge
            kinesisClient.putRecordsExplicitHashKey(
                mergedShardCount batchesOf RECORD_COUNT,
                predefinedShards = childShards
            )
        }
    }

    /**
     * Test for the scenario where any node get replaced.
     */
    @Timeout(value = 4, timeUnit = TimeUnit.MINUTES)
    @Test
    internal fun redeployment_scenario(testContext: VertxTestContext) {
        val nodeCount = 8
        val nodeScoreAllNodesRunning = 4
        val shardCountAndOverallNodeScore = nodeCount * nodeScoreAllNodesRunning
        val nodeCountToStopPerRedeploymentStep = 4
        val runningNodesAfterStop = nodeCount - nodeCountToStopPerRedeploymentStep
        val redeploymentSteps = nodeCount / nodeCountToStopPerRedeploymentStep

        testContext.async(RECORD_COUNT * shardCountAndOverallNodeScore) { checkpoint ->
            // Create stream with initial shard count and deploy nodes
            val streamDescription = createStreamAndDeployNodes(nodeCount, shardCountAndOverallNodeScore)

            // Create node score service and await any node has consumer expected consumer count started
            nodeScoreService().awaitScore(shardCountAndOverallNodeScore, nodeScoreAllNodesRunning)

            repeat(redeploymentSteps) {
                // We stop the amount of node according redeployment step size and await re-balancing
                nodeVertxInstances.takeAndRemove(nodeCountToStopPerRedeploymentStep).forEach { vertx -> vertx.close().await() }
                nodeScoreService().awaitScore(shardCountAndOverallNodeScore, shardCountAndOverallNodeScore / runningNodesAfterStop)

                // We start the amount of node according redeployment step size and await re-balancing
                deployNodes(nodeCountToStopPerRedeploymentStep, streamDescription)
                nodeScoreService().awaitScore(shardCountAndOverallNodeScore, nodeScoreAllNodesRunning)
            }

            // Register fan out consumers on each node
            nodeVertxInstances.forEach { vertx ->
                vertx.eventBus().consumer<Int>(RECORD_FAN_OUT_ADDR) { msg ->
                    val recordCount = msg.body()
                    repeat(recordCount) { checkpoint.flag() }
                }.completion().await()
            }

            // Send records even to each shard
            val shards = streamDescription.shards()
            kinesisClient.putRecordsExplicitHashKey(
                shardCountAndOverallNodeScore batchesOf RECORD_COUNT, predefinedShards = shards
            )
        }
    }

    private fun nodeScoreService() = NodeScoreService.createService(nodeVertxInstances.last())

    private suspend fun createStreamAndDeployNodes(instances: Int, shardCount: Int): StreamDescription {
        val streamDescription = kinesisClient.createAndGetStreamDescriptionWhenActive(shardCount)
        deployNodes(instances, streamDescription)
        return streamDescription
    }

    private suspend fun deployNodes(instances: Int, streamDescription: StreamDescription) {
        repeat(instances) {
            val vertx = Vertx.vertx()
            vertx.setUpKinesisClient()
            val orchestraOptions = VertxKinesisOrchestraOptions(
                applicationName = TEST_APPLICATION_NAME,
                streamName = TEST_STREAM_NAME,
                region = AWS_REGION,
                credentialsProviderSupplier = { AWS_CREDENTIALS_PROVIDER },
                consumerVerticleClass = MultiNodeComponentTestConsumerVerticle::class.java.name,
                redisOptions = redisHeimdallOptions,
                fetcherOptions = fetcherOptions(streamDescription)
            )
            VertxKinesisOrchestra.create(vertx, orchestraOptions).start().await()
            nodeVertxInstances.add(vertx)
        }
    }

    protected abstract fun fetcherOptions(streamDescription: StreamDescription): FetcherOptions

    private suspend fun NodeScoreService.awaitScore(
        expectedOverallScore: Int,
        expectedNodeScore: Int = expectedOverallScore
    ) {
        var expectedScoreReached = false
        while (!expectedScoreReached) {
            val nodeScores = getNodeScores().await()
            val nodeScoreSum = nodeScores.sumOf { it.score }
            val allNodeHaveExpectedScore = nodeScores.map { it.score }.all { it == expectedNodeScore }
            expectedScoreReached = nodeScoreSum == expectedOverallScore && allNodeHaveExpectedScore
            delay(1000)
        }
    }

    abstract fun Vertx.setUpKinesisClient()
}

class MultiNodeComponentTestConsumerVerticle : AbstractKinesisConsumerCoroutineVerticle() {
    override suspend fun onRecordsAsync(records: List<Record>) {
        vertx.eventBus().send(AbstractComponentTest.RECORD_FAN_OUT_ADDR, records.size)
    }
}