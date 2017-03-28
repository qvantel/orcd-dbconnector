package se.qvantel.connector

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}

class SparkConnectionTest extends FlatSpec with BeforeAndAfter {


  private val master = "local[2]"
  private val appName = "exampleSpark"
  private var sc: SparkContext = _




  before{
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
    sc = new SparkContext("local[2]", "database", conf)
  }

  after{
    if(sc != null){
      sc.stop()
    }
  }
}
