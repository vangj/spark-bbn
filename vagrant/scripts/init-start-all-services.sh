#!/bin/bash
source "/vagrant/scripts/common.sh"

function formatNameNode {
	$HADOOP_PREFIX/bin/hdfs namenode -format myhadoop -force -noninteractive
	echo "formatted namenode"
}

function startHDFS {
	$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
	$HADOOP_PREFIX/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
	echo "started hdfs"
}

function startYarn {
	ssh node2 '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager'
	ssh node2 '$HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager'
	ssh node2 '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start proxyserver'
	ssh node2 '$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver'
	echo "started yarn"
}

function createEventLogDir {
	$HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp
	$HADOOP_PREFIX/bin/hdfs dfs -mkdir /tmp/spark-events
	echo "created spark event log dir"
}

function startSpark {
	$SPARK_HOME/sbin/start-all.sh
	$SPARK_HOME/sbin/start-history-server.sh
	echo "started spark"
}

formatNameNode
startHDFS
startYarn
createEventLogDir
startSpark