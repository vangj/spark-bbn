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

# References

* https://github.com/scoverage/sbt-scoverage
* https://www.scala-sbt.org/release/docs/Using-Sonatype.html
