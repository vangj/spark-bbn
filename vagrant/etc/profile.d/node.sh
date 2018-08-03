#!/bin/bash

export JAVA_HOME=/usr/local/java
export HADOOP_PREFIX=/usr/local/hadoop
export HADOOP_YARN_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export YARN_LOG_DIR=$HADOOP_YARN_HOME/logs
export YARN_IDENT_STRING=root
export HADOOP_MAPRED_IDENT_STRING=root
export SPARK_HOME=/usr/local/spark
export ZK_HOME=/usr/local/zookeeper
export ZOOKEEPER_HOME=/usr/local/zookeeper
export KAFKA_HOME=/usr/local/kafka

export PATH=$JAVA_HOME/bin:$PATH
export PATH=$HADOOP_PREFIX/bin:$PATH
export PATH=$SPARK_HOME/bin:$PATH
export PATH=$ZOOKEEPER_HOME/bin:$PATH
export PATH=$KAFKA_HOME/bin:$PATH