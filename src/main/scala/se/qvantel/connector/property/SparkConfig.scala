package se.qvantel.connector.property


trait SparkConfig extends Config {
  val hostName = config.getString("spark.cassandra.host")
  val userName = config.getString("spark.cassandra.username")
  val password = config.getString("spark.cassandra.password")
  val keySpace = config.getString("spark.cassandra.keyspace")
}
