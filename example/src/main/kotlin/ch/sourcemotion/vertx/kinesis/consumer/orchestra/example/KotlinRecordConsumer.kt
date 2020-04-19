package ch.sourcemotion.vertx.kinesis.consumer.orchestra.example

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.consumer.AbstractKinesisConsumerCoroutineVerticle
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.core.eventbus.requestAwait
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import software.amazon.awssdk.services.kinesis.model.Record
import kotlin.math.ceil


/**
 * Consumer verticle which in fact is a dispatcher of records.
 * The chunks of record get sent to [KinesisRecordFanOutVerticle]
 */
class KotlinRecordConsumer : AbstractKinesisConsumerCoroutineVerticle() {

    private val options by lazy { config.mapTo(KotlinRecordConsumerOptions::class.java) }

    override suspend fun onRecordsAsync(records: List<Record>) {
        if (records.isEmpty()) {
            return
        }
        val recordChunks = createRecordDataChunks(records)

        // We sent a request for each chunk of record data. The number of chunks should compare to the number of fan out
        // verticles, so we would have may the best efficiency.
        val chunkJobs = recordChunks.map { chunk ->
            async { vertx.eventBus().requestAwait<Unit>(options.fanOutAddress, chunk) }
        }
        // We wait on this line, as the end of the coroutine is the signal to the orchestra to deliver the next bunch of records.
        awaitAll(*chunkJobs.toTypedArray())
    }

    /**
     * @return Chunks of data on consumed records. The list get mapped to Vertx [JsonArray] so we are able to send them
     * over the event bus.
     */
    private fun createRecordDataChunks(records: List<Record>) =
        records.chunked(ceil((records.size / options.fanOutTargetCount).toDouble()).toInt())
            .map { JsonArray(it.map { record -> record.data().asUtf8String() }) }
}

/**
 * We have to ignore unknown as technically, it will get merged with the internal consumer configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class KotlinRecordConsumerOptions(
    val fanOutAddress: String,
    val fanOutTargetCount: Int
)
