package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region

object Localstack {
    const val VERSION = "0.10.8"

    val credentialsProvider: StaticCredentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key"))
    val region: Region = Region.EU_WEST_1
}

fun LocalStackContainer.getKinesisEndpointOverride() = "${getEndpointOverride(LocalStackContainer.Service.KINESIS)}"
fun LocalStackContainer.getDynamoDBEndpointOverride() = "${getEndpointOverride(LocalStackContainer.Service.DYNAMODB)}"
