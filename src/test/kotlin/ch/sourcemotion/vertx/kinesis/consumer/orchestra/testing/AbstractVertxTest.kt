package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.codec.OrchestraCodecs
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.registerKinesisOrchestraModules
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.eventbus.EventBus
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.atomic.AtomicInteger

@ExtendWith(VertxExtension::class)
abstract class AbstractVertxTest {

    @Volatile
    protected lateinit var vertx: Vertx

    @Volatile
    protected lateinit var context: Context

    @Volatile
    protected lateinit var defaultTestScope: CoroutineScope

    @Volatile
    protected lateinit var eventBus: EventBus

    @BeforeEach
    internal fun setUpVertx(vertx: Vertx) {
        DatabindCodec.mapper().registerKinesisOrchestraModules()
        DatabindCodec.prettyMapper().registerKinesisOrchestraModules()
        this.vertx = vertx
        eventBus = vertx.eventBus()
        OrchestraCodecs.deployCodecs(eventBus)

        context = vertx.orCreateContext
        defaultTestScope = CoroutineScope(context.dispatcher())
    }


    protected fun asyncTest(testContext: VertxTestContext, block: suspend () -> Unit) {
        defaultTestScope.launch {
            runCatching { block() }
                .onSuccess { testContext.completeNow() }
                .onFailure { testContext.failNow(it) }
        }
    }

    protected fun asyncTest(
        testContext: VertxTestContext,
        checkpoint: Checkpoint,
        block: suspend (Checkpoint) -> Unit
    ) {
        defaultTestScope.launch {
            runCatching { block(checkpoint) }
                .onSuccess { checkpoint.flag() }
                .onFailure { testContext.failNow(it) }
        }
    }

    protected fun asyncTest(
        testContext: VertxTestContext,
        checkpoints: Int,
        block: suspend (Checkpoint) -> Unit
    ) {
        asyncTest(testContext, testContext.checkpoint(checkpoints + 1), block)
    }

    /**
     * Async test delegate function with a delayed, so the test will not end with the last call of [Checkpoint.flag]
     * on the test checkpoint, but on call [Checkpoint.flag] on control checkpoint.
     * This way too many calls on the test checkpoint will result in failing test.
     */
    protected fun asyncTestDelayedEnd(
        testContext: VertxTestContext,
        checkpoints: Int,
        delayMillis: Long = 2000,
        block: suspend (Checkpoint) -> Unit
    ) {
        val doubleCheckpoint = DoubleCheckpoint.create(vertx, testContext, checkpoints, delayMillis)
        defaultTestScope.launch {
            runCatching { block(doubleCheckpoint) }
                .onFailure { testContext.failNow(it) }
        }
    }
}

private class DoubleCheckpoint private constructor(
    requiredNumberOfPasses: Int,
    private val vertx: Vertx,
    private val testCheckpoint: Checkpoint,
    private val controlCheckpoint: Checkpoint,
    private val delayMillis: Long
) : Checkpoint {

    companion object {
        fun create(
            vertx: Vertx,
            testContext: VertxTestContext,
            requiredNumberOfPasses: Int,
            delayMillis: Long
        ): Checkpoint {
            return DoubleCheckpoint(
                requiredNumberOfPasses,
                vertx,
                testContext.checkpoint(requiredNumberOfPasses),
                testContext.checkpoint(),
                delayMillis
            )
        }
    }

    private val missingPasses = AtomicInteger(requiredNumberOfPasses)

    override fun flag() {
        testCheckpoint.flag()
        if (missingPasses.decrementAndGet() == 0) {
            vertx.setTimer(delayMillis) {
                controlCheckpoint.flag()
            }
        }
    }
}
