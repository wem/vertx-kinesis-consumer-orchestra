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
