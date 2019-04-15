Initialised Kafka
=====

https://kafka.apache.org/quickstart
Download to a local directory:
`tar -xzf kafka_2.11-2.1.0.tgz`
`cd kafka_2.11-2.1.0`

Start Zookeeper from within the local kafka folder
`bin/zookeeper-server-start.sh config/zookeeper.properties`

Once it has settled, start Kafka from the same place
`bin/kafka-server-start.sh config/server.properties`

Create topics:
`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic seed-topic`
`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic target-topic`

Note: you may have an issue with the java kafka apps not being able to talk to the kafka instance.
If so, add the following line to kafka's `configs/server.properties` file:
`listeners=PLAINTEXT://localhost:9092`

Build
=====

In the root of the producer folder build the project with 
`./gradlew fatJar`
Then execute the created jar with 
`java -jar build/libs/kafka-producer-1.0.jar`

This will by default produce 1 event to the seed topic
To add multiple events you can use, eg
`java -jar build/libs/kafka-producer-1.0.jar --count 10`
You can also provide an `--interval` argument to set the millisecond delay between event emissions. Defaults to 100ms interval.

To build the consumer in the processor folder execute
`./gradlew fatJar`
Then execute the created jar with 
`java -jar build/libs/kafka-processor-1.0.jar`

To confirm emitted events, attach a console consumer back in the kafka directory:
`bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic target-topic --from-beginning`
or to confirm input events:
`bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic seed-topic --from-beginning`
