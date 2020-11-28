package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region

object Localstack {
    val dockerImage: DockerImageName = DockerImageName.parse("localstack/localstack:0.12.2")

    val credentialsProvider: StaticCredentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"))
    val region: Region = Region.EU_WEST_1
}

fun LocalStackContainer.getKinesisEndpointOverride() = "${getEndpointOverride(LocalStackContainer.Service.KINESIS)}"
fun LocalStackContainer.getDynamoDBEndpointOverride() = "${getEndpointOverride(LocalStackContainer.Service.DYNAMODB)}"
