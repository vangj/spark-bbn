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
	$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
	$HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager
	$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start proxyserver
	$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver
	echo "started yarn"
}

function createEventLogDir {
	$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /tmp/spark-events
	echo "created spark event log dir"
}

function startSpark {
	$SPARK_HOME/sbin/start-all.sh
	$SPARK_HOME/sbin/start-history-server.sh
	echo "started spark"
}

function copyData {
	echo "copying over data files"
	cp -f /vagrant/data/* /home/vagrant/
	$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/vagrant/data
	$HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal /home/vagrant/data-1479668986461.csv /user/vagrant/data
}

formatNameNode
startHDFS
startYarn
createEventLogDir
startSpark
copyData