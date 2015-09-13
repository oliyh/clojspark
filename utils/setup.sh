#!/bin/bash

# Create HDFS folders:
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/dummy
sudo -u hdfs hadoop fs -chown -R dummy:dummy hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -chown -R dummy:dummy hdfs://localhost/user/dummy

# Copy the input file into the HDFS folders
hadoop fs -copyFromLocal input/wordcount-input.txt hdfs://localhost/user/clojspark/basics/inputdata

# Build Scala Package
cd scala
sbt package;

# Submit the job on YARN
spark-submit --class basics.SparkWordCount --master yarn target/scala-2.10/clojspark_2.10-0.1-SNAPSHOT.jar

# same example in Clojure
cd ../clojure
lein run
