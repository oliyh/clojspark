#!/bin/bash

# Create HDFS folders:
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/dummy
sudo -u hdfs hadoop fs -chown -R dummy:dummy hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -chown -R dummy:dummy hdfs://localhost/user/dummy

# Copy the input file into the HDFS folders
hadoop fs -copyFromLocal -f input/house-prices.csv hdfs://localhost/user/clojspark/basics/inputdata/house-prices.csv
hadoop fs -copyFromLocal -f input/house-prices-small.csv hdfs://localhost/user/clojspark/basics/inputdata/house-prices-small.csv

cd clojure
lein uberjar
spark-submit --class spark.core --master yarn target/sparkling-getting-started-1.0.0-SNAPSHOT-standalone.jar
