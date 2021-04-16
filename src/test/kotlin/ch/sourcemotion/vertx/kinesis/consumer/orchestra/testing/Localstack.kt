package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region

object Localstack {
    /**
     * TODO: Replace with regular release when 0.12.9.1+ is released.
     */
    val dockerImage: DockerImageName = DockerImageName.parse("localstack/localstack:latest")
//    val dockerImage: DockerImageName = DockerImageName.parse("localstack/localstack:0.12.9.1")

    val credentialsProvider: StaticCredentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"))
    val region: Region = Region.US_EAST_1
}

fun LocalStackContainer.getKinesisEndpointOverrideUri() = "${getEndpointOverride(LocalStackContainer.Service.KINESIS)}"
fun LocalStackContainer.getDynamoDBEndpointOverride() = "${getEndpointOverride(LocalStackContainer.Service.DYNAMODB)}"
