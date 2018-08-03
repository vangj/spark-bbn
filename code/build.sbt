import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.github.vangj",
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.11.8",
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  credentials += Credentials(Path.userHome / ".ivy2" / ".ossrh-credentials"),
  pomExtra := (
    <url>http://gitlab.jeeforce.io/data/dq-spark</url>
      <scm>
        <url>git@github.com:vangj/spark-bbn.git</url>
        <connection>scm:git:git@github.com:vangj/spark-bbn.git</connection>
        <developerConnection>scm:git:git@github.com:vangj/spark-bbn.git</developerConnection>
      </scm>
      <developers>
        <developer>
          <email>vangjee@gmail.com</email>
          <name>Jee Vang, Ph.D.</name>
          <url>https://github.com/vangj</url>
          <id>vangj</id>
          <organization>Jee Vang</organization>
          <organizationUrl>https://github.com/vangj</organizationUrl>
        </developer>
      </developers>)
)

lazy val commonDeps = Seq(
  junit,
  specs,
  sparkCore,
  sparkSql,
  sparktesting,
  scalatest,
  sparkCsv,
  csvparser,
  scopt,
  esspark,
  scalaz
)

lazy val app = (project in file("."))
  .settings(commonSettings: _*)
  .settings(pgpPassphrase := scala.util.Properties.envOrNone("gpgpassphrase").map(_.toCharArray))
  .settings(name := "spark-bbn")
  .settings(libraryDependencies ++= commonDeps)
  .settings(parallelExecution in Test := false)
  .settings(javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"))
  .settings(scalacOptions ++= Seq("-feature", "-deprecation"))
  .settings(test in assembly := {})
  .settings(assemblyMergeStrategy in assembly :=  {
  case ps if ps.endsWith(".SF") => MergeStrategy.discard
  case ps if ps.endsWith(".DSA") => MergeStrategy.discard
  case ps if ps.endsWith(".RSA") => MergeStrategy.discard
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
})
