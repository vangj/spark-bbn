#!/bin/bash
source "/vagrant/scripts/common.sh"

function setupHadoop {
	echo "creating hadoop directories"
	mkdir /var/hadoop
	mkdir /var/hadoop/hadoop-datanode
	mkdir /var/hadoop/hadoop-namenode
	mkdir /var/hadoop/mr-history
	mkdir /var/hadoop/mr-history/done
	mkdir /var/hadoop/mr-history/tmp
	
	echo "copying over hadoop configuration files"
	cp -f /vagrant/conf/hadoop/* /usr/local/hadoop/etc/hadoop
}

function installHadoop {
	echo "install hadoop"
	FILE=/vagrant/resources/hadoop-2.7.3.tar.gz
	tar -xzf $FILE -C /usr/local
	ln -s /usr/local/hadoop-2.7.3 /usr/local/hadoop
}


echo "setup hadoop"
installHadoop
setupHadoop