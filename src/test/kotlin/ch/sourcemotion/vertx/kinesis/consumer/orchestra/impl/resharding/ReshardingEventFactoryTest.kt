package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.resharding

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.shardIdTyped
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.streamDescriptionWhenActiveAwait
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractKinesisAndRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.ShardIdGenerator
import io.kotlintest.matchers.collections.shouldContainAll
import io.kotlintest.matchers.types.shouldBeInstanceOf
import io.kotlintest.shouldBe
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Test

internal class ReshardingEventFactoryTest : AbstractKinesisAndRedisTest() {

    @Test
    internal fun split_shard(testContext: VertxTestContext) = asyncTest(testContext) {
        val streamDescription = createAndGetStreamDescriptionWhenActive(1)
        val parentShard = streamDescription.shards().first()

        splitShardFair(parentShard)

        val sut = ReshardingEventFactory(
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME),
            TEST_STREAM_NAME,
            parentShard.shardIdTyped()
        )

        val reshardingEvent = sut.createReshardingEvent()
        reshardingEvent.reshardingType.shouldBe(ReshardingType.SPLIT)
        reshardingEvent.shouldBeInstanceOf<SplitReshardingEvent> {
            it.parentShardId.shouldBe(parentShard.shardIdTyped())
            it.childShardIds.shouldContainAll(ShardIdGenerator.generateShardIdList(2, 1))
        }
    }

    @Test
    internal fun merge_shards(testContext: VertxTestContext) = asyncTest(testContext) {

        val streamDescription = createAndGetStreamDescriptionWhenActive(2)
        val parentShard = streamDescription.shards().first()
        val adjacentShard = streamDescription.shards()[1]

        mergeShards(parentShard, adjacentShard)

        val sut = ReshardingEventFactory(
            kinesisClient.streamDescriptionWhenActiveAwait(TEST_STREAM_NAME),
            TEST_STREAM_NAME,
            parentShard.shardIdTyped()
        )

        val reshardingEvent = sut.createReshardingEvent()
        reshardingEvent.reshardingType.shouldBe(ReshardingType.MERGE)
        reshardingEvent.shouldBeInstanceOf<MergeReshardingEvent> {
            it.parentShardId.shouldBe(parentShard.shardIdTyped())
            it.adjacentParentShardId.shouldBe(adjacentShard.shardIdTyped())
            it.childShardId.shouldBe(ShardIdGenerator.generateShardId(2))
        }
    }
}
