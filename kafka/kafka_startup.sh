#!/bin/bash

# wait for zookepper to start
until nc -z zookeeper 2181; do
    echo "wait for zookeeper to start ..."
    sleep 3
done 

# wait for kafka to start
until nc -z kafka 9092; do
    echo "wait for kafka to start ..."
    sleep 3
done

# start the kafka producer
echo "starting kafka producer ..."
exec python kafka_producer.py 