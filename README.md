# Spark BBN

Spark-BBN is an API to use Spark to learn Bayesian Belief Networks (BBNs).

# HOWTO use Spark BBN

For now, only a singly-connected tree BBN is supported. Assuming you have a CSV file in HDFS, you could submit a Spark job as follows.

```
$SPARK_HOME/bin/spark-submit --class com.github.vangj.bbn.tool.BbnMstLearner \
    --master spark://node1:7077 \
    spark-bbn-assembly-0.0.1-SNAPSHOT.jar \
    --i /user/vagrant/data/data-1479668986461.csv \
    --o /tmp/001-graph \
    --omi /tmp/001-mi
```

Or, more generically.

```
$SPARK_HOME/bin/spark-submit --class com.github.vangj.bbn.tool.BbnMstLearner \
    --master spark://[master-node]:[master-node-port] \
    spark-bbn-assembly-0.0.1-SNAPSHOT.jar \
    --i /path/to/some.csv \
    --o /path/to/graph-output-folder \
    --omi /path/to/mutual-information-output-folder
```

You may copy and paste the output of the JSON graph directly into [this example](https://run.plnkr.co/plunks/GFcem156HC2EwRECmtyH/) and visualize the BBN via [jsbayes-viz](https://github.com/vangj/jsbayes-viz).

# HOWTO use Spark

You are also able to use the Spark cluster from your host machine. 

* Unzip the Spark and Hadoop archives locally and link them.
  - `ln -s hadoop-2.7.3/ hadoop`
  - `ln -s spark-2.1.0-bin-hadoop2.7/ spark`
* Copy the configuration files over.
* Modify `spark-env.sh` and `hadoop-env.sh` by setting `JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home` (on Mac).
* Modify `.bash_profile` by adding the following.
  - `SPARK_HOME=~/dev/spark`
  - `HADOOP_HOME=~/dev/hadoop`
  - `HADOOP_PREFIX=~/dev/hadoop`
  - `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_161.jdk/Contents/Home`
  - `export HADOOP_CONF_DIR=~/dev/hadoop/etc/hadoop`

# Citation

```
@misc{vang_spark_bbn_2018, 
title={Spark BBN}, 
url={https://github.com/vangj/spark-bbn/}, 
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