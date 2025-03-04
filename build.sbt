ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "Data-Engineering-Portfolio-Streaming"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"            % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  // Delta Lake
  "io.delta"        %% "delta-spark"           % "3.2.0",
  // ScalaTest
  "org.scalatest"   %% "scalatest"             % "3.2.19" % Test,
  // Spark Fast Tests
  "com.github.mrpowers" %% "spark-fast-tests"  % "1.3.0" % Test
)
