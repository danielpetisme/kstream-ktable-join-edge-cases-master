# kstream-ktable-join-edge-cases

## TL;DR
* Kafka Stream join are not deterministic in sub-topology context (ie. repartitioning before a join)
* `.through()` create a new stream within the current topology thus can't solve the above point
* Whenever there is a repartition operation (`selectKey`, `transform`, etc.) you shouldn't chain a `join` operation. (ie. create dedicated topologies)
* Timestamp synchronizationis dependant of the order Kafka subscriptions polling order thus the timestamp comparison should be strict (ie. `<` strict not `<=`).
  

## About

A suite of KStream-KTable join edge cases spotted during the Stock Booster (SEB).

Have a look to the test `KStreamKTableJoinWithTimestampExtractorTest`.

## Usage

The project runs with Java 11 and build with Maven project.
Uses [HAP config files](https://gitlab.michelin.com/DEV/config-file) to configure Michelin Artifactory repositories if you can't access to the Internet.

```
./mvnw clean test
```

