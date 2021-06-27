# Changelog

## [0.7.0]
### Added
#### Custom shard state persistence
Make it possible to use custom shard state persistence `ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService`. So the users are able now to use another persistence
for the shard state : https://github.com/wem/vertx-kinesis-consumer-orchestra/wiki/Shard-state-persistence

### Improved
#### Redis connection handling
The VKCO is using exactly 2 connection to Redis by default. The one on the default implementation of `ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService`
has now proper reconnecting capabilities.
 
### Fixed
#### #11 Ignore non existing parents on orchestration
When the VKCO fetches shards from Kinesis to orchestrate, (parent) shards they no more exists are ignored now. Previously VKCO
only did visit its own state (shard state persistence).

## [0.8.0]
### Added
#### AWS client metrics
VKCO supports now Kinesis client metrics. Thanks to Michael Wegener for the PR!

#### KCL v1 importer
It's now possible to continue consuming from the last known "point" of a previous KCL v1 consumer.

#### Dynamic fetcher
One "criteria" of the response round-trip time of Kinesis relates to the record limit provided in the request.
E.g. Assumption: The next request returns 100 record. The round-trip with a limit of 2000 will be much longer than if we
define a limit of 200. So the new dynamic fetcher will adjust the "next limit" based on the most recent response.

### Improved
#### Consumer deployment
The consumer deployment (per shard) massively simplified. There is a "not consumed shard detection", which will initiate the deployment of a consumer for a shard automatically when detected

#### Resharding process
The resharding process significantly simplified. On merge the child shard get directly consumed on the VKCO instance of the latest finished parent. 
In the case of split, the first child get directly consumed by the same VKCO instance as the parent was consumed. 
The second child will get deployed by the "not consumed shard detection".

## [0.8.1]
### Fixed
#### #23 Race condition in consumable shard detection
The interaction between consumable shard detection and consumer control hardened and simplified. 
The detection verticle known possible count of consumer to start now updated only by event from consumer control.

#### Consumable shard list
Correction of the consumable shard list creation. Will not contain children of unavailable (still consumed) parents any more.

### Improved
#### Renaming
- NotConsumedShardDetectorVerticle to ConsumableShardDetectionVerticle
- ConsumerShardIdListFactory to ConsumableShardIdListFactory

#### Logging
Consumer start / stop log entries should now be more clear and clean, especially on resharding.

## [0.8.2]
### Fixed
#### #24 Consumable detection not working on child shards if parents are no more existing.
No more existing parents get now considered on consumable shard detection.

### Improved
#### Verticle undeploy order on close
The verticles are now undeployed in reversed order as they got deployed. 
This will ensure that any verticle is available as long as it's needed.

#### Cleanup
- Change modifier of some classes to internal.
- Remove unused code.

## [0.8.3]
### Improved
#### #25 Parameterizable HttpClientOptions for Vert.x AWS SDK
It's now possible to configure custom Vert.x http client options to access Kinesis.

## [0.0.9.0]
**Note about the version number, the final release would get called 0.9.0 afterwards when related Localstack issues are fixed and tests adjusted.**
### Feature
#### Enhanced fanout support
The VKCO now supports fetching records from Kinesis with the enhanced fanout mechanism. It's important to note that:
- The enhanced fanout support currently works only with the AWS SDK built-in Netty client because of Http/2 configuration / behavior 
  issues with the Vert.x client https://github.com/wem/vertx-kinesis-consumer-orchestra/issues/26.
- Because of https://github.com/localstack/localstack/issues/3822 and https://github.com/localstack/localstack/issues/3823 the tests are limited at this time. 
  Especially record cardinality / order tests (e.g. failure handling) are basically difficult when only LATEST shard iterator is available.
  
Take a look at `ch.sourcemotion.vertx.kinesis.consumer.orchestra.VertxKinesisOrchestraOptions` /
`ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions` /
`ch.sourcemotion.vertx.kinesis.consumer.orchestra.EnhancedFanOutOptions` how to use and configure.
  
#### Fetcher metrics
VKCO now supports fetcher records counter metrics (applied only on enhanced fanout at the moment). 
Please check `ch.sourcemotion.vertx.kinesis.consumer.orchestra.FetcherOptions`

## [0.0.9.1]
### Maintenance
#### Moved to Maven central
Because Bintray will become recently deprecated, this project is now available on Maven central. Please check README.
### Improved
Some minor fixes and improvements.

## [0.0.9.2]
### Fixed
#### #27 Use of SCAN instead of KEYS
To get a list of keys from Redis we now strictly use SCAN instead of KEYS to avoid longer blocked server.
### Maintenance
#### Moved from Localstack back to AWS
Localstack lacks of some features, especially on the Kinesis service. E.g. childShards variables not filled on record events or responses.
So from now, the tests are more representative and valid.
### Improved
#### Error handling
If a record processing error will get thrown back to VKCO, we don't read the records again, but directly re-deliver.
#### Resharding
Resharding mechanism / workflow improved and hardened. Only a few of requests against Kinesis used during resharding.
#### Limit exceeded handled
At some points we could run into some Kinesis request count per time limits. This is now (better) handled.

## [0.0.9.3]
### Fixed
#### Too large batches
On some circumstances, the batch size can grove near limitless. Now the delivered batch size is limited consumer side to recordsPreFetchLimit as well.  
#### Parallel subscriber requests on Enhanced fanout
Workaround removed which did cover the scenario when Localstack did not deliver records immediately after first request.  
### Maintenance
#### Cleanup
Unused resetTo function removed on fetcher(s).

## [0.0.9.4]
### Fixed
#### Use MGET instead of SCAN
We now use MGET to obtain information about shards they are finished or in progress.   
### Improved
#### Limit handling
Request limit handling on listShards requests.
#### Record queuing / buffering
We replaced Channel from Kotlin SDK with own suspendable queue implementation because of memory problem in some situations (needs "more" investigation). 
#### Resharding
Replaced internal running state of the AbstractKinesisConsumerVerticle with two (fetching, inProgress), for hardening of the resharding process. 
#### Consumable shard detection
Introduction of a backoff for the detection interval to spread the detection and deployment of consumable shards. 
#### Timeout handling during consumer deployment
Hardening of the timeout handling during consumer deployment. 
### Maintenance
#### AWS SDK version
Bumped AWS SDK to 2.16.85

## [0.0.9.5]
### Fixed / Maintenance
Bumped AWS SDK to 2.16.88 and Vert.x to 3.9.8 (4.1.65.Final) because of potential Netty incompatibilities.