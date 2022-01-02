package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.balancing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisConsumerOrchestraException

class BalancingException(message: String? = null, cause: Throwable? = null) :
    VertxKinesisConsumerOrchestraException(message, cause)