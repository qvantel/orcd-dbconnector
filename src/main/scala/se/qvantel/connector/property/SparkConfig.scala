package se.qvantel.connector.property

trait SparkConfig extends Config {
  val hostName = config.getString("spark.cassandra.host")
  val userName = config.getString("spark.cassandra.username")
  val cassandraPort = config.getString("spark.cassandra.c.port")
  val password = config.getString("spark.cassandra.password")
  val keySpace = config.getString("spark.cassandra.keyspace")
  val cdrTable = config.getString("cassandra.cdrTable")
  val cdrSyncTable = config.getString("cassandra.cdrSyncTable")
}
