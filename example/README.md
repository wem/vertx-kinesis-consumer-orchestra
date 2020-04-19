# vertx-kinesis-consumer-orchestra example

This examples (Kotlin and Java versions (Java is pending)) should show how to use the orchestra.
The examples are implemented in a fan-out fashion, means the final consumer of record chunks is
`ch.sourcemotion.vertx.kinesis.consumer.orchestra.example.KinesisRecordFanOutVerticle`. There are 2 instances deployed of
this verticle and so 2 chunks (they are fair split) of records can be proceeded.

1'000'000 records will get put and consumed.

## How to start

- Run docker-compose up in the infrastructure folder in this example project. This will start a Localstack (Kinesis only) 
and Redis container.
- Start the main in `ch.sourcemotion.vertx.kinesis.consumer.orchestra.example.KotlinExample`
