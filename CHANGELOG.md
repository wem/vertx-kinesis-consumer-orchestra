# Changelog

## [0.7.0]
### Added
#### Custom shard state persistence
- Make it possible to use custom shard state persistence `ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService`. So the users are able now to use another persistence
for the shard state : https://github.com/wem/vertx-kinesis-consumer-orchestra/wiki/Shard-state-persistence

### Improved
#### Redis connection handling
- The VKCO is using exactly 2 connection to Redis by default. The one on the default implementation of `ch.sourcemotion.vertx.kinesis.consumer.orchestra.spi.ShardStatePersistenceService`
has now proper reconnecting capabilities.
 
### Fixed
#### #11 Ignore non existing parents on orchestration
- When the VKCO fetches shards from Kinesis to orchestrate, (parent) shards they no more exists are ignored now. Previously VKCO
only did visit its own state (shard state persistence).
