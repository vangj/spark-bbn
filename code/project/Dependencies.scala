import sbt._

object Dependencies {
	val junit = "junit" % "junit" % "4.12" % Test
	val specs = "org.specs2" %% "specs2" % "2.4" % Test
	val scalatest = "org.scalatest" %% "scalatest" % "2.2.6" % Test
	val sparktesting = "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % Test
	val sparkCore = "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
	val sparkSql = "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
	val sparkCsv = "com.databricks" %% "spark-csv" % "1.5.0"
	val esspark = "org.elasticsearch" %% "elasticsearch-spark-20" % "5.1.1" intransitive
	val csvparser = "com.opencsv" % "opencsv" % "3.8"
	val scopt = "com.github.scopt" %% "scopt" % "3.2.0"
	val scalaz = "org.scalaz" %% "scalaz-core" % "7.1.11"
	val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided"
}
