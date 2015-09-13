# clojspark

This repo is to demonstrate how to run a simple word Count example using Spark (in Yarn client mode) with a Scala and a Clojure example which are strictly equivalent. The user is invited to compare both implementation and extend this repo with more Spark example. The repo was created to be available to bootstrap a Clojure dojo taking place in Canary Wharf London on the 15th Sept 2015.

## Pre-requisite

* the [Docker Cloudera image](https://github.com/smdahmed/hadoop-cdh-pseudo-docker) is built and started as Docker container

## Quick Start

* ssh into your the [Docker Cloudera image](https://github.com/smdahmed/hadoop-cdh-pseudo-docker) that you have started
* `cd /dummy/dev`
* Clone this repo (`git clone git@github.com:smdahmed/clojspark.git`) inside the [Docker Cloudera image](https://github.com/smdahmed/hadoop-cdh-pseudo-docker)
* `cd clojspark`
* run:

```
./utils/setup.sh
```
