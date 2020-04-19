package ch.sourcemotion.vertx.kinesis.consumer.orchestra.example

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext.ack
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.coroutines.CoroutineVerticle
import mu.KLogging

/**
 * Fan-out verticle. There are multiple instances deployed on Vert.x. So all those instances together represents the fan-out
 */
class KinesisRecordFanOutVerticle : CoroutineVerticle(), Handler<Message<JsonArray>> {

    private companion object : KLogging()

    private val options by lazy { config.mapTo(KinesisRecordFanOutVerticleOptions::class.java) }

    private var totalReceivedRecord = 0

    override suspend fun start() {
        vertx.eventBus().consumer(options.consumingAddress, this::handle)
        logger.info { "Fan-out verticle ready to receive record data chunks" }
    }

    override fun handle(event: Message<JsonArray>) {
        val recordBunchSize = event.body().size()
        totalReceivedRecord += recordBunchSize
        logger.info { "Received a chunk of records with size ${recordBunchSize}. Total received records: $totalReceivedRecord" }
        event.ack()
    }
}

class KinesisRecordFanOutVerticleOptions(
    val consumingAddress: String
)
