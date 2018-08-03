#!/bin/bash
source "/vagrant/scripts/common.sh"

function installJava {
	echo "installing oracle jdk"
	FILE=/vagrant/resources/jdk-8u111-linux-x64.gz
	tar -xzf $FILE -C /usr/local
	ln -s /usr/local/jdk1.8.0_111 /usr/local/java
}

echo "setup java"
installJava