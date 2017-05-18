#! /bin/bash
cd /opt/iot/
/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 --class EventGenerator --driver-java-options "-Dlog4j.configuration=file:///opt/iot/log4j.properties" /opt/iot/IoTEvents-assembly-1.0.jar
