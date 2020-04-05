package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.shard

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.redis.RedisKeyFactory
import io.vertx.core.shareddata.Shareable
import io.vertx.redis.client.RedisAPI
import java.time.Duration

/**
 * Factory of [ShardStatePersistence] instances. This factory can be preconfigured, so later [Context] specific instances
 * can be fabricated.
 */
class ShardStatePersistenceFactory(
    private val shardProgressExpiration: Duration,
    private val redisKeyFactory: RedisKeyFactory
) : Shareable {

    companion object {
        const val SHARED_DATA_REF = "shard-state-persistence-factory"
    }

    fun createShardStatePersistence(redisApi: RedisAPI): ShardStatePersistence {
        return ShardStatePersistence(
            redisApi,
            shardProgressExpiration,
            redisKeyFactory
        )
    }
}
