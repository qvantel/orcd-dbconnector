package se.qvantel.connector

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import se.qvantel.connector.property.Config



trait SparkConnection extends Config{
  // Configure spark->cassandra connection
  var host = "spark.cassandra.connection.host"
  var usrName = "spark.cassandra.auth.username"
  var pwd = "spark.cassandra.auth.password"
  val conf = new SparkConf(true)
    .set(host, config.getString(host))
    .set(usrName, config.getString(usrName))
    .set(pwd, config.getString(pwd))
  val context = new SparkContext("local[2]", "database", conf)

  // Setup cassandra connector
  val connector = CassandraConnector(conf)
  // Create cassandra session
  val session = connector.openSession()

  // create new tables for syncing
  session.execute("CREATE TABLE IF NOT EXISTS qvantel.cdrsync(id INT PRIMARY KEY, ts timestamp)")
}

