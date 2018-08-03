#!/bin/bash
source "/vagrant/scripts/common.sh"

function disableFirewall {
	echo "disabling firewall"
	service iptables save
	service iptables stop
	chkconfig iptables off
}

function setupEnv {
	echo "settingn up env variables"
	cp -f /vagrant/etc/profile.d/node.sh /etc/profile.d/node.sh
	source /etc/profile.d/node.sh
}

echo "setup centos"

disableFirewall
setupEnv