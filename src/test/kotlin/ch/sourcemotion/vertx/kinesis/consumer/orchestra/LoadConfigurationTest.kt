package ch.sourcemotion.vertx.kinesis.consumer.orchestra

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class LoadConfigurationTest {

    @Test
    internal fun consume_all_shards() {
        val sut = LoadConfiguration.createConsumeAllShards()
        sut.maxShardsCount.shouldBe(Int.MAX_VALUE)
    }

    @Test
    internal fun consume_exact_without_over_commit() {
        val sut = LoadConfiguration.createConsumeExact(5)
        sut.maxShardsCount.shouldBe(5)
    }

    @Test
    internal fun consume_exact_with_over_commit() {
        val sut = LoadConfiguration.createConsumeExact(5, 2)
        sut.maxShardsCount.shouldBe(5)
    }

    @Test
    internal fun consume_exact_illegal_arguments() {
        shouldThrow<VertxKinesisConsumerOrchestraException> { LoadConfiguration.createConsumeExact(0) }
    }
}
