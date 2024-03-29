package ch.sourcemotion.vertx.kinesis.consumer.orchestra.testing

import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ShardId
import ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.importer.KCLV1Importer
import kotlinx.coroutines.future.await
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

suspend fun DynamoDbAsyncClient.deleteTableIfExists(tableName: String) {
    runCatching { describeTable { it.tableName(tableName) }.await() }
        .onSuccess {
            deleteTable { builder -> builder.tableName(tableName) }.await()
        }

    var deleted = false
    while (deleted.not()) {
        runCatching { describeTable { it.tableName(tableName) }.await() }
            .onFailure {
                if (it is ResourceNotFoundException) {
                    deleted = true
                }
            }
    }
}

suspend fun DynamoDbAsyncClient.forceCreateLeaseTable(tableName: String) {
    deleteTableIfExists(tableName)
    createTable { builder ->
        builder.tableName(tableName).keySchema(
            KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(KCLV1Importer.LEASE_KEY_ATTR)
                .build()
        ).attributeDefinitions(
            AttributeDefinition.builder().attributeName(KCLV1Importer.LEASE_KEY_ATTR)
                .attributeType(ScalarAttributeType.S).build()
        ).provisionedThroughput(
            ProvisionedThroughput.builder().writeCapacityUnits(5).readCapacityUnits(5).build()
        )
    }.await()
    awaitTableActive(tableName)
}

private suspend fun DynamoDbAsyncClient.awaitTableActive(tableName: String) {
    var status = describeTable { it.tableName(tableName) }.await().table().tableStatus()
    while (status != TableStatus.ACTIVE) {
        status = describeTable { it.tableName(tableName) }.await().table().tableStatus()
    }
}

suspend fun DynamoDbAsyncClient.putLeases(tableName: String, vararg leases: Pair<ShardId, String>) {
    transactWriteItems { txBuilder ->
        val items = leases.map { lease ->
            TransactWriteItem.builder().put { putBuilder ->
                putBuilder.tableName(tableName)
                putBuilder.item(
                    mapOf(
                        KCLV1Importer.LEASE_KEY_ATTR to AttributeValue.builder().s("${lease.first}").build(),
                        KCLV1Importer.CHECKPOINT_ATTR to AttributeValue.builder().s(lease.second).build()
                    )
                )
            }.build()
        }
        txBuilder.transactItems(items)
    }.await()
}
