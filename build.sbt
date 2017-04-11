name := "QvantelDBConnector"

version := "1.0"

scalaVersion := "2.11.8"

assemblyJarName in assembly := "DBConnector.jar"
mainClass in assembly := Some("se.qvantel.connector.DBConnector")

lazy val execScript = taskKey[Unit]("Download mcc library")

execScript := {
  import sys.process._
  Seq("./get_latest_mcc_table.bash") !
}

compile in Compile <<= (compile in Compile).dependsOn(execScript)
logLevel in assembly := Level.Error
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "compile",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "compile",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3",
  "org.json4s" %% "json4s-native" % "3.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
)

// This part is required for spark to assemble
// Why? I don't know, but it works
// http://stackoverflow.com/a/31618903
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
