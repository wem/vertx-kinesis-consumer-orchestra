package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.ShardIteratorStrategy
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.KinesisConsumerVerticleOptions
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.internal.service.ConsumerControlService
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractRedisTest
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_APPLICATION_NAME
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.TEST_STREAM_NAME
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.ints.shouldBeZero
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.core.AbstractVerticle
import io.vertx.core.Verticle
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import java.time.Duration

internal class ConsumerControlVerticleTest : AbstractRedisTest() {

    companion object {
        const val START_ACK_ADDR = "/consumer-control/test/start"
        const val STOP_ACK_ADDR = "/consumer-control/test/stop"
    }

    @Test
    internal fun start_one_consumer(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val startEventChannel = eventBusChannelOf<Unit>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds(1)).await().shouldBe(1)
        startEventChannel.receive()
    }

    @Test
    internal fun start_consumer_same_shard_two_times(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardIds = shardIds(1)
        val startEventChannel = eventBusChannelOf<Unit>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBe(1)
        startEventChannel.receive()
        // Second start should be ignored
        consumerControl.startConsumers(shardIds).await().shouldBe(1)
        delay(1000)
        startEventChannel.tryReceive().getOrNull().shouldBeNull()
    }

    @Test
    internal fun start_ten_consumers(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardIds = shardIds(10)
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBe(shardIds.size)
        shardIds.forEach { shardId ->
            startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(
                shardId
            )
        }
    }

    @Test
    internal fun start_ten_consumers_than_stop_each(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardIds = shardIds(10)
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        val stopEventChannel = eventBusChannelOf<JsonObject>(STOP_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBe(shardIds.size)
        shardIds.forEach { shardId ->
            startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java)
                .shardId.shouldBe(shardId)
        }

        shardIds.forEachIndexed { idx, shardId ->
            val stopConsumersCmdResult = consumerControl.stopConsumers(1).await()
            stopConsumersCmdResult.activeConsumers.shouldBe(shardIds.size - (idx + 1))
            stopConsumersCmdResult.stoppedShardIds.shouldContainExactly(shardId)

            stopEventChannel.receive().body()
                .mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(shardId)
        }
    }

    @Test
    internal fun stop_not_started_consumers(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val startedShardId = ShardId("1")
        val notStartedShardId = ShardId("2")

        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        val stopEventChannel = eventBusChannelOf<JsonObject>(STOP_ACK_ADDR)

        consumerControl.startConsumers(listOf(startedShardId)).await().shouldBe(1)
        startEventChannel.receive()

        // Try to stop consumer of a shard for that never a consumer was started
        consumerControl.stopConsumer(notStartedShardId)
        delay(1000)
        stopEventChannel.tryReceive().getOrNull().shouldBeNull()
    }

    @Test
    internal fun start_stop_start_consumer(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardId = ShardId("1")

        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        val stopEventChannel = eventBusChannelOf<JsonObject>(STOP_ACK_ADDR)

        consumerControl.startConsumers(listOf(shardId)).await().shouldBe(1)
        startEventChannel.receive()

        consumerControl.stopConsumer(shardId)
        stopEventChannel.receive()

        consumerControl.startConsumers(listOf(shardId)).await().shouldBe(1)
        startEventChannel.receive()
    }

    @Test
    internal fun stop_more_than_started_consumers(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardIds = shardIds(10)
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBe(shardIds.size)
        shardIds.forEach { shardId ->
            startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(
                shardId
            )
        }

        val stopEventChannel = eventBusChannelOf<JsonObject>(STOP_ACK_ADDR)
        val stopConsumersCmdResult = consumerControl.stopConsumers(20).await()
        stopConsumersCmdResult.stoppedShardIds.shouldContainExactlyInAnyOrder(shardIds)
        stopConsumersCmdResult.activeConsumers.shouldBeZero()
        shardIds.forEach { shardId ->
            stopEventChannel.receive().body()
                .mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(shardId)
        }
        // Verify only 10 consumers are stopped
        delay(1000)
        stopEventChannel.tryReceive().getOrNull().shouldBeNull()
    }

    @Test
    internal fun force_latest_when_no_existing_sequence_number(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardId = ShardId("1")
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(listOf(shardId)).await().shouldBe(1)
        val options = startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java)
        options.shardId.shouldBe(shardId)
        options.shardIteratorStrategy.shouldBe(ShardIteratorStrategy.FORCE_LATEST)
    }

    @Test
    internal fun existing_or_latest_on_existing_sequence_number(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartStopAckConsumerVerticle::class.java))
        val shardId = ShardId("1")
        shardStatePersistenceService.saveConsumerShardSequenceNumber(shardId, SequenceNumber("1", SequenceNumberIteratorPosition.AT))
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(listOf(shardId)).await().shouldBe(1)
        val options = startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java)
        options.shardId.shouldBe(shardId)
        options.shardIteratorStrategy.shouldBe(ShardIteratorStrategy.EXISTING_OR_LATEST)
    }

    @Test
    internal fun stop_failing_consumers(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StopFailingConsumerVerticle::class.java))
        val shardIds = shardIds(10)
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBe(shardIds.size)
        shardIds.forEach { shardId ->
            startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(
                shardId
            )
        }

        val stopEventChannel = eventBusChannelOf<JsonObject>(STOP_ACK_ADDR)

        val stopConsumersCmdResult = consumerControl.stopConsumers(shardIds.size).await()
        stopConsumersCmdResult.stoppedShardIds.shouldContainExactlyInAnyOrder(shardIds)
        stopConsumersCmdResult.activeConsumers.shouldBeZero()
        shardIds.forEach { shardId ->
            stopEventChannel.receive().body()
                .mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(shardId)
        }
    }

    @Test
    internal fun start_failing_consumers(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartFailingConsumerVerticle::class.java))
        val shardIds = shardIds(10)
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBeZero()
        shardIds.forEach { shardId ->
            startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(
                shardId
            )
        }
    }

    @Test
    internal fun start_timed_out_consumers(testContext: VertxTestContext) = testContext.async {
        val consumerControl = deployConsumerControl(consumerControlOptionsOf(StartTimeoutConsumerVerticle::class.java))
        val shardIds = shardIds(10)
        val startEventChannel = eventBusChannelOf<JsonObject>(START_ACK_ADDR)
        consumerControl.startConsumers(shardIds).await().shouldBeZero()
        shardIds.forEach { shardId ->
            startEventChannel.receive().body().mapTo(KinesisConsumerVerticleOptions::class.java).shardId.shouldBe(
                shardId
            )
        }
    }

    private fun shardIds(amount: Int) = IntRange(1, amount).map { ShardId("$it") }

    private suspend fun deployConsumerControl(
        options: ConsumerControlVerticle.Options
    ): ConsumerControlService {
        deployTestVerticle<ConsumerControlVerticle>(options)
        return ConsumerControlService.createService(vertx)
    }

    private fun consumerControlOptionsOf(
        verticleClass: Class<out Verticle>,
        verticleOptions: JsonObject = JsonObject(),
        consumerDeploymentTimeoutMillis: Long = 1000,
    ) =
        VertxKinesisOrchestraOptions(
            applicationName = TEST_APPLICATION_NAME,
            streamName = TEST_STREAM_NAME,
            redisOptions = redisHeimdallOptions,
            consumerVerticleClass = verticleClass.name,
            consumerVerticleOptions = verticleOptions,
            shardIteratorStrategy = ShardIteratorStrategy.EXISTING_OR_LATEST,
            consumerDeploymentTimeout = Duration.ofMillis(consumerDeploymentTimeoutMillis)
        ).asConsumerControlOptions()
}

class StartStopAckConsumerVerticle : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().send(ConsumerControlVerticleTest.START_ACK_ADDR, config())
    }

    override fun stop() {
        vertx.eventBus().send(ConsumerControlVerticleTest.STOP_ACK_ADDR, config())
    }
}

class StopFailingConsumerVerticle : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().send(ConsumerControlVerticleTest.START_ACK_ADDR, config())
    }

    override fun stop() {
        vertx.eventBus().send(ConsumerControlVerticleTest.STOP_ACK_ADDR, config())
        throw Exception("Some failure")
    }
}

class StartFailingConsumerVerticle : AbstractVerticle() {
    override fun start() {
        vertx.eventBus().send(ConsumerControlVerticleTest.START_ACK_ADDR, config())
        throw Exception("Some failure")
    }

    override fun stop() {
        vertx.eventBus().send(ConsumerControlVerticleTest.STOP_ACK_ADDR, config())
    }
}

class StartTimeoutConsumerVerticle : CoroutineVerticle() {
    override suspend fun start() {
        vertx.eventBus().send(ConsumerControlVerticleTest.START_ACK_ADDR, config)
        delay(10000)
    }
}