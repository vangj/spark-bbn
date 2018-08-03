# Spark-BBN

Spark-BBN is a Spark API for learning Bayesian Belief Networks (BBNs).

# HOWTO build

To create the assembly via sbt or maven.

```
sbt assembly
mvn package
```

To publish via sbt or maven. make sure your credentials are set.

* `~/.ivy2/.ossrh-credentials` for sbt
* `settings.xml` for maven

```
sbt publish
mvn deploy
```
# HOWTO generate code coverage

```
sbt clean coverage test
sbt coverageReport
```

# Citation

```
@misc{vang_spark_bbn_2018, 
title={Spark BBN}, 
url={https://github.com/vangj/spark-bbn/code/}, 
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
