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
  - `export PYSPARK_PYTHON=/usr/local/bin/python2.7`


# Create a Conda environment

To create a Conda environment.

```
conda create -n spark python=2.7 anaconda
source activate spark
pip install -r requirements.txt
python -m ipykernel install --user --name spark --display-name "spark"
```

To remove `conda env remove -n spark --yes`.
