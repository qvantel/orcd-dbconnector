name := "QvantelDBConnector"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % "2.1.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.scala-lang.modules" %% "scala-pickling" % "0.10.1")