package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension.SingletonContainer
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension.SingletonContainerExtension
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB
import org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS

@ExtendWith(SingletonContainerExtension::class)
interface LocalstackContainerTest {
    companion object {
        @JvmStatic
        @get:SingletonContainer
        var localStackContainer: LocalStackContainer = LocalStackContainer(Localstack.dockerImage)
            .withServices(DYNAMODB, KINESIS)
    }

    fun getDynamoDbEndpointOverrideUri() = localStackContainer.getEndpointOverride(DYNAMODB)
    fun getKinesisEndpointOverrideUri() = localStackContainer.getEndpointOverride(KINESIS)
    fun getDynamoDbEndpointOverride() = "${localStackContainer.getEndpointOverride(DYNAMODB)}"
    fun getKinesisEndpointOverride() = "${localStackContainer.getEndpointOverride(KINESIS)}"
}
