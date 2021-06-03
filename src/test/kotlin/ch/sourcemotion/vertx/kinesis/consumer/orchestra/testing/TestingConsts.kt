package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.OrchestraClusterName

const val LEASE_TABLE_NAME = "vkco-kcl-lease-test"
const val TEST_STREAM_NAME = "vkco-test-stream"
const val TEST_APPLICATION_NAME = "vkco-test-application"

internal val TEST_CLUSTER_ORCHESTRA_NAME = OrchestraClusterName(TEST_APPLICATION_NAME, TEST_STREAM_NAME)
