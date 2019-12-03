#!/bin/bash

# start kafka prodcuer
echo "***	STARTING ZOOKEEPER:"
zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

echo "***	STARTING KAFKA SERVER:"
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

echo "***	STARTING HDFS:"
start-dfs.sh

sleep 5

start-yarn.sh

sleep 5

echo "***	STARTING HIVE METASTORE:"
hive --service metastore

sleep 5

echo "***	STARTING KAFKA PRODUCER:"
python3 ./kafka-producer.py

echo "*** SUBMITTING TO SPARK-STREAMING:"
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark_streaming.py
