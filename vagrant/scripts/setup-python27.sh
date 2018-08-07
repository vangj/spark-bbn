#!/bin/bash
# https://myopswork.com/install-python-2-7-10-on-centos-rhel-75f90c5239a5
source "/vagrant/scripts/common.sh"

function installPython {
	echo "install python from local file"
	FILE=/vagrant/resources/Python-2.7.15.tgz
	tar -xzf $FILE -C /tmp
	cd /tmp/Python-2.7.15
    ./configure
    sudo make altinstall
}

installPython