#!/bin/bash

hadoop fs -rm -r /PA3/out/
mvn clean compile
mvn package
spark-submit --class com.cs435.Main --supervise ./target/PageRank-1.0-SNAPSHOT.jar