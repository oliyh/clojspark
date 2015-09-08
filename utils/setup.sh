#!/bin/bash

# Create HDFS folders:
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/root
sudo -u hdfs hadoop fs -chown -R root:root hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -chown -R root:root hdfs://localhost/user/root

# Copy the input file into the HDFS folders
hadoop fs -copyFromLocal input/wordcount-input.txt hdfs://localhost/user/clojspark/basics/inputdata

# Build Scala Package
sbt package;

# Submit the job on YARN
spark-submit --class basics.SparkWordCount --master yarn target/scala-2.10/clojspark_2.10-0.1-SNAPSHOT.jar
