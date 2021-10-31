## How to run?

### Start Servers

Start kafka server and zookeeper:

```sh
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Start Cassandra in the foreground:

```sh
$CASSANDRA_HOME/bin/cassandra -f
```

### Data Processing using Kafka and Structure Streaming

Check list of kafka topics:

```sh
kafka-topics.sh --list --zookeeper localhost:2181
```

If topic 'covid' is not in list, create 'covid' topic:

```sh
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic covid
```

In /task1, run built.sbt, start structure streaming processing application for our first task:

```sh
sbt run
```

In /task2, run built.sbt, start structure streaming processing application for our second task:

```sh
sbt run
```

In /producer, run built.sbt, get messages from data.csv and feed them to topic 'covid':

```sh
sbt run
```

Check the messages sent to topic 'covid':

```sh
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic covid --from-beginning
```

Delete unwanted topic, eg. "covid":

```sh
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic covid
```

### Check results in Cassandra

Task1 and task2 results will be automatically stored in Cassandra. To check the results, open a terminal and enter cql console by:

```sh
cqlsh
```

Check all keyspaces:

```CQL
describe keyspaces;
```

Go to our results' keyspace and check including tables:

```CQL
use covid;
describe tables;
```

Task1 results are in table "status", access them by:

```CQL
select * from status;
```

Task2 results are in table "elder_status", access them by:

```CQL
select * from elder_status;
```

Delete unwanted keyspace, eg. "covid":

```CQL
DROP KEYSPACE "covid";
```

### Visualization using Pyecharts

In /visualization, run jupyter notebook "Project-Cassandra.ipynb". 

