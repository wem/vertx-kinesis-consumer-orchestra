package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension.SingletonContainer
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing.extension.SingletonContainerExtension
import mu.KLogging
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB
import org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS
import org.testcontainers.containers.output.Slf4jLogConsumer

@ExtendWith(SingletonContainerExtension::class)
interface LocalstackContainerTest {
    companion object : KLogging() {

        private val containerLogger = Slf4jLogConsumer(logger).withSeparateOutputStreams()

        @JvmStatic
        @get:SingletonContainer
        var localStackContainer: LocalStackContainer = LocalStackContainer(Localstack.dockerImage)
            .withServices(DYNAMODB, KINESIS)
    }

    @BeforeEach
    fun setUpLocalstackLogging() {
        localStackContainer.followOutput(containerLogger)
    }

    fun getDynamoDbEndpointOverrideUri() = localStackContainer.getEndpointOverride(DYNAMODB)
    fun getKinesisEndpointOverrideUri() = localStackContainer.getEndpointOverride(KINESIS)
    fun getDynamoDbEndpointOverride() = "${localStackContainer.getEndpointOverride(DYNAMODB)}"
    fun getKinesisEndpointOverride() = "${localStackContainer.getEndpointOverride(KINESIS)}"
}
