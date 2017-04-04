package se.qvantel.connector

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import se.qvantel.connector.property.SparkConfig


trait SparkConnection extends SparkConfig {
  // Configure spark->cassandra connection
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", hostName)
    .set("spark.cassandra.auth.username", userName)
    .set("spark.cassandra.auth.password", password)
  val context = new SparkContext("local[2]", "database", conf)

  // Setup cassandra connector
  val connector = CassandraConnector(conf)
  // Create cassandra session
  val session = connector.openSession()

  // create new tables for syncing
  session.execute("CREATE TABLE IF NOT EXISTS " + config.getString("spark.cassandra.keyspace") + ".cdrsync(id INT PRIMARY KEY, ts timestamp)")
}

