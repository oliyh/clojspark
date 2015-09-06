#!/bin/bash

# Create HDFS folders:
hadoop fs -mkdir -p hdfs://localhost/user/clojspark/basics/inputdata

# Copy the input file into the HDFS folders
hadoop fs -copyFromLocal input/wordcount-input.txt hdfs://localhost/user/clojspark/basics/inputdata

# Build Scala Package
sbt package;

# Submit the job on YARN
spark-submit --class basics.SparkWordCount --master yarn target/scala-2.10/clojspark_2.10-0.1-SNAPSHOT.jar
