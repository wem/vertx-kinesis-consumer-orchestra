package ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.fetching

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.AbstractVertxTest
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldHaveAtMostSize
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.SdkBytes

internal class ResponseEntryQueueTest : AbstractVertxTest() {

    @Test
    internal fun send_suspended_if_limit_reached(testContext: VertxTestContext) = testContext.async {
        val limit = 10
        val sut = ResponseEntryQueue(limit)
        val entries = responseEntryListOf(limit + 1)
        defaultTestScope.launch {
            sut.send(entries)
        }
        delay(100)
        sut.size().shouldBe(limit)
        sut.senderSuspended().shouldBeTrue()
    }

    @Test
    internal fun receiver_suspended_if_empty(testContext: VertxTestContext) = testContext.async {
        val limit = 10
        val sut = ResponseEntryQueue(limit)
        defaultTestScope.launch {
            sut.receive()
        }
        delay(100)
        sut.size().shouldBe(0)
        sut.isEmpty().shouldBeTrue()
        sut.receiverSuspended().shouldBeTrue()
    }

    @Test
    internal fun concurrent_send_receive(testContext: VertxTestContext) {
        val expectedElements = 100
        testContext.async(expectedElements) { checkpoint ->
            val limit = 10
            val sut = ResponseEntryQueue(limit)
            defaultTestScope.launch {
                repeat(limit) {
                    defaultTestScope.launch {
                        sut.send(responseEntryListOf(limit))
                    }.join()
                }
            }
            defaultTestScope.launch {
                val receivedElements = ArrayList<ResponseEntry>()
                while (receivedElements.size < expectedElements) {
                    val elements = sut.receive()
                    testContext.verify { elements.shouldHaveAtMostSize(limit) }
                    receivedElements += elements
                    repeat(elements.size) { checkpoint.flag() }
                }
            }
        }
    }

    @Test
    internal fun single_element_send(testContext: VertxTestContext) {
        val expectedElements = 100
        testContext.async(expectedElements) { checkpoint ->
            val limit = 10
            val sut = ResponseEntryQueue(limit)
            defaultTestScope.launch {
                repeat(100) {
                    defaultTestScope.launch {
                        sut.send(responseEntryListOf(1))
                    }.join()
                }
            }
            defaultTestScope.launch {
                val receivedElements = ArrayList<ResponseEntry>()
                while (receivedElements.size < expectedElements) {
                    val elements = sut.receive()
                    testContext.verify { elements.shouldHaveAtMostSize(limit) }
                    receivedElements += elements
                    repeat(elements.size) { checkpoint.flag() }
                }
            }
        }
    }

    @Test
    internal fun delay_before_send(testContext: VertxTestContext) {
        val expectedElements = 100
        testContext.async(expectedElements) { checkpoint ->
            val limit = 10
            val sut = ResponseEntryQueue(limit)
            defaultTestScope.launch {
                repeat(100) {
                    delay(10)
                    defaultTestScope.launch {
                        sut.send(responseEntryListOf(1))
                    }.join()
                }
            }
            defaultTestScope.launch {
                val receivedElements = ArrayList<ResponseEntry>()
                while (receivedElements.size < expectedElements) {
                    val elements = sut.receive()
                    testContext.verify { elements.shouldHaveAtMostSize(limit) }
                    receivedElements += elements
                    repeat(elements.size) { checkpoint.flag() }
                }
            }
        }
    }

    @Test
    internal fun send_and_receive(testContext: VertxTestContext) {
        val expectedElements = 100
        testContext.async(expectedElements) { checkpoint ->
            val sut = ResponseEntryQueue(expectedElements)
            sut.send(responseEntryListOf(100))

            val receivedElements = ArrayList<ResponseEntry>()
            while (receivedElements.size < expectedElements) {
                val elements = sut.receive()
                testContext.verify { elements.shouldHaveSize(expectedElements) }
                receivedElements += elements
                receivedElements.validateEntryOrdering()
                repeat(elements.size) { checkpoint.flag() }
            }
        }
    }

    @Test
    internal fun send_and_receive_1000000_elements(testContext: VertxTestContext) {
        val expectedElements = 1000000
        val responseEntries = responseEntryListOf(100)
        testContext.async(expectedElements) { checkpoint ->
            val sut = ResponseEntryQueue(100)
            val sendTask = {
                defaultTestScope.launch {
                    sut.send(responseEntries)
                }
            }
            defaultTestScope.launch {
                while (true) {
                    val elements = sut.receive()
                    repeat(elements.size) { checkpoint.flag() }
                }
            }
            repeat(10000) {
                sendTask().join()
            }
        }
    }

    private fun List<ResponseEntry>.validateEntryOrdering() {
        forEachIndexed { idx, responseEntry ->
            responseEntry.record.shouldNotBeNull().data().asUtf8String().shouldBe("$idx")
        }
    }

    private fun responseEntryListOf(elements: Int): List<ResponseEntry> = IntRange(0, elements - 1).map { idx ->
        ResponseEntry(mock {
            on { data() } doReturn SdkBytes.fromUtf8String("$idx")
        }, mock(), emptyList(), 0)
    }
}