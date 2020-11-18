package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName

const val TEST_STREAM_NAME = "test-stream"
const val TEST_APPLICATION_NAME = "test-application"

val TEST_CLUSTER_ORCHESTRA_NAME = OrchestraClusterName(TEST_APPLICATION_NAME, TEST_STREAM_NAME)
