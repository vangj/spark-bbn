# Vagrant with Hadoop v2.7.3 and Spark v2.1.0

# Introduction

Vagrant project to spin up a cluster of 4 virtual machines with Hadoop v2.7.3 and Spark v2.1.0.

1. node1 : HDFS NameNode + Spark Master
2. node2 : YARN ResourceManager + JobHistoryServer + ProxyServer
3. node3 : HDFS DataNode + YARN NodeManager + Spark Slave
4. node4 : HDFS DataNode + YARN NodeManager + Spark Slave

# Getting Started

1. [Download and install VirtualBox](https://www.virtualbox.org/wiki/Downloads)
2. [Download and install Vagrant](http://www.vagrantup.com/downloads.html).
3. Run ```vagrant box add centos65 https://github.com/2creatives/vagrant-centos/releases/download/v6.5.1/centos65-x86_64-20131205.box```
4. Git clone this project, and change directory (cd) into this project (directory).
5. Run ```vagrant up``` to create the VM.
6. Run ```vagrant ssh``` to get into your VM.
7. Run ```vagrant destroy``` when you want to destroy and get rid of the VM.

# Advanced Stuff

If you have the resources (CPU + Disk Space + Memory), you may modify Vagrantfile to have even more HDFS DataNodes, YARN NodeManagers, and Spark slaves. Just find the line that says "numNodes = 4" in Vagrantfile and increase that number. The scripts should dynamically provision the additional slaves for you.

# Requirements of Artifacts
You will have to have the following files in the  `/resources` directory.

1. /resources/hadoop-2.7.3.tar.gz
2. /resources/spark-2.1.0-bin-hadoop2.7.tgz
3. /resources/jdk-8u65-linux-x64.gz
4. /resources/sshpass-1.05-1.el6.rf.x86_64.rpm

# Make sure YARN and Spark jobs can run
I typically run the following tests after post-provisioning on node1 (as root user). 

```
vagrant ssh node1
```

## Test YARN
Run the following command to make sure you can run a MapReduce job.

```
yarn jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar pi 2 100
```

## Test Spark on YARN
You can test if Spark can run on YARN by issuing the following command. Try NOT to run this command on the slave nodes.

```
$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --num-executors 10 \
    --executor-cores 2 \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    100
```

## Test code directly on Spark	
```
$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master spark://node1:7077 \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    100
```
	
## Test Spark using Shell
Start the Spark shell using the following command. Try NOT to run this command on the slave nodes.

```
$SPARK_HOME/bin/spark-shell --master spark://node1:7077
```

Then go here https://spark.apache.org/docs/latest/quick-start.html to start the tutorial. Most likely, you will have to load data into HDFS to make the tutorial work (Spark cannot read data on the local file system).

# Modify /etc/hosts file

You need to add an entry to your hosts file, as we will be referencing the vm by name.

```
10.211.55.101 node1
10.211.55.102 node2
10.211.55.103 node3
10.211.55.104 node4
10.211.55.105 node5
10.211.55.106 node6
```

# Web UI
You can check the following URLs to monitor the Hadoop daemons.

1. [NameNode] (http://node1:50070/dfshealth.html)
2. [ResourceManager] (http://node2:8088/cluster)
3. [JobHistory] (http://node2:19888/jobhistory)
4. [Spark] (http://node1:8080)
5. [Spark History] (http://node1:18080)

# Vagrant boxes
A list of available Vagrant boxes is shown at http://www.vagrantbox.es. 

# Vagrant box location
The Vagrant box is downloaded to the ~/.vagrant.d/boxes directory. On Windows, this is C:/Users/{your-username}/.vagrant.d/boxes.

# Citation

```
@misc{vang_spark_bbn_vagrant_2018, 
title={Spark BBN Vagrant}, 
url={https://github.com/vangj/spark-bbn/vagrant/}, 
journal={GitHub},
author={Vang, Jee}, 
year={2018}, 
month={Aug}}
```

# Copyright Stuff

```
Copyright 2018 Jee Vang

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```