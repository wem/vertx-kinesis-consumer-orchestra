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

@ExtendWith(VertxExtension::class)
abstract class AbstractVertxTest {

    protected lateinit var vertx: Vertx
    protected lateinit var context: Context
    protected lateinit var defaultTestScope: CoroutineScope
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
     * Same as [asyncTest(testContext, checkpoint, block)] but with lax checkpoint.
     */
    protected fun asyncTestMin(
        testContext: VertxTestContext,
        checkpoints: Int,
        block: suspend (Checkpoint) -> Unit
    ) {
        asyncTest(testContext, testContext.laxCheckpoint(checkpoints + 1), block)
    }
}
