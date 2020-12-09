package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RollingIntListTest {

    @Test
    internal fun size_not_exceed_capacity_and_rolling() {
        val sut = RollingIntList(3)
        // Fill list to capacity limit
        IntRange(0, 2).forEach { sut.add(it) }
        sut.size.shouldBe(3)
        sut.full.shouldBeTrue()
        // Add element so capacity limit exceeds
        sut.add(3)
        sut.size.shouldBe(3)
        sut.full.shouldBeTrue()
        IntRange(1, 3).forEach { sut.contains(it).shouldBeTrue() }
        // Add element so capacity limit exceeds
        sut.add(4)
        sut.size.shouldBe(3)
        sut.full.shouldBeTrue()
        IntRange(2, 4).forEach { sut.contains(it).shouldBeTrue() }
    }
}
