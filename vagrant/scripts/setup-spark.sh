#!/bin/bash
source "/vagrant/scripts/common.sh"

function setupSpark {
	echo "setup spark"
	cp -f /vagrant/conf/spark/* /usr/local/spark/conf
}

function installSpark {
	echo "install spark from local file"
	FILE=/vagrant/resources/spark-2.1.0-bin-hadoop2.7.tgz
	tar -xzf $FILE -C /usr/local
	ln -s /usr/local/spark-2.1.0-bin-hadoop2.7 /usr/local/spark
}

installSpark
setupSpark